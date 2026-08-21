"""Airflow DAG: load, validate, branch, quarantine, aggregate.

```
ensure_schema -> load_feed -> validate -> choose_path
                                             |-- failed --> quarantine_rejects --> alert
                                             |-- passed --> quality_passed
                                                                    |
                                                       refresh_aggregates (either path)
```

The branch is the point of the DAG rather than decoration. ``refresh_aggregates``
reads from ``v_valid_trade``, so aggregation only ever sees rows that passed the
rules; when validation fails, the quarantine task runs first so that what was excluded
is recorded before anything downstream depends on its absence.

Every task here is a thin wrapper over a function in ``tradedq`` or ``tradepnl``. That
is deliberate: business logic inside an Airflow task can only be tested by running
Airflow, which is slow enough that in practice it does not get tested at all.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import uuid
from pathlib import Path
from typing import Any

# Airflow 3 moved DAG authoring to airflow.sdk. The 2.x path still works in 3.x but is
# deprecated, so prefer the new one and fall back rather than pinning to one major.
try:
    from airflow.sdk import dag, task
except ImportError:  # pragma: no cover - Airflow 2.x
    from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# A connection string from the environment keeps this runnable with docker compose and
# nothing else. A production deployment would use an Airflow Connection backed by a
# secrets manager, so the credential is not visible in `docker inspect`.
DATABASE_URL = os.environ.get(
    "TRADEDQ_DATABASE_URL",
    "postgresql+psycopg://trades:trades@postgres:5432/trades",
)

GX_PROJECT_ROOT = os.environ.get("TRADEDQ_GX_ROOT", "/opt/airflow/gx-project")


def _engine():  # noqa: ANN202 - SQLAlchemy Engine
    from sqlalchemy import create_engine

    return create_engine(DATABASE_URL, pool_pre_ping=True, future=True)


@dag(
    dag_id="trade_data_quality",
    description="Validate the trade feed, quarantine bad rows, then aggregate.",
    schedule="@daily",
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": dt.timedelta(minutes=2)},
    tags=["data-quality", "trades"],
)
def trade_data_quality() -> None:
    """Daily data quality run over the trade table."""

    @task
    def ensure_schema() -> None:
        """Create both projects' tables. Idempotent, so it is safe on every run."""
        from tradepnl.db import create_schema as create_pipeline_schema

        from tradedq.schema import create_schema as create_quality_schema

        engine = _engine()
        create_pipeline_schema(engine)
        create_quality_schema(engine)

    @task
    def load_feed() -> dict[str, Any]:
        """Generate a feed and upsert it.

        A real deployment would read a file dropped by an upstream system. Generating
        it here keeps the DAG self-contained so someone can run it without needing
        access to a trade feed.

        The load is deliberately full rather than incremental. The generator re-emits
        the same year of history on every run, so it behaves like a source that
        re-presents its whole snapshot, not like a feed of new trades. Running it
        incrementally would skip almost everything on the second run -- correct for a
        real feed, misleading here -- and the upsert is what makes re-presenting the
        same rows harmless. The incremental path is exercised by the pipeline project.
        """
        from tradepnl.cli import seed_instruments
        from tradepnl.generate import build_instruments, generate_trades
        from tradepnl.load import load_trades

        engine = _engine()
        seed_instruments(engine)
        rows = generate_trades(build_instruments(), count=20_000)
        result = load_trades(
            engine, rows, source_file="airflow_generated", incremental=False
        )
        log.info("load: %s", result.summary())
        return {"rows_upserted": result.rows_upserted}

    @task
    def validate() -> dict[str, Any]:
        """Run the rules and record the run. Does not fail the task on bad data.

        Bad data is an expected outcome, not an error. Raising here would mean the
        branch never executes and the quarantine never happens, which is precisely
        the failure mode this pipeline exists to avoid.
        """
        from tradedq.runs import start_run
        from tradedq.validate import run_validation

        engine = _engine()
        run_id = uuid.uuid4()
        start_run(engine, run_id)

        outcome = run_validation(
            engine,
            DATABASE_URL,
            project_root=Path(GX_PROJECT_ROOT),
            run_id=run_id,
        )
        log.info("validation: %s", outcome.summary())
        return {
            "run_id": str(outcome.run_id),
            "success": outcome.success,
            "failed_rules": outcome.failed_rules,
            "rows_checked": outcome.rows_checked,
            "rules_total": outcome.rules_total,
            "details": outcome.details,
        }

    @task.branch
    def choose_path(outcome: dict[str, Any]) -> str:
        """Send the run down the quarantine path only when something actually failed."""
        if outcome["success"]:
            log.info("all rules passed, skipping quarantine")
            return "quality_passed"
        log.warning("failed rules: %s", ", ".join(outcome["failed_rules"]))
        return "quarantine_rejects"

    @task
    def quarantine_rejects(outcome: dict[str, Any]) -> dict[str, Any]:
        """Copy offending rows into rejected_trades with the reasons they failed."""
        from tradedq.quarantine import quarantine_failing_rows
        from tradedq.rules import build_rules

        engine = _engine()
        result = quarantine_failing_rows(
            engine, build_rules(), run_id=uuid.UUID(outcome["run_id"])
        )
        log.warning("quarantine: %s", result.summary())
        return {
            "rows_rejected": result.rows_rejected,
            "reason_counts": result.reason_counts,
        }

    @task
    def alert(quarantined: dict[str, Any]) -> None:
        """Where a real deployment would page somebody.

        Deliberately a log line: wiring Slack or PagerDuty into a portfolio project
        adds a credential and a dependency without demonstrating anything further.
        """
        counts = quarantined["reason_counts"]
        worst = max(counts, key=counts.get) if counts else "none"
        log.warning(
            "DATA QUALITY ALERT: %s rows quarantined, most common reason %s",
            quarantined["rows_rejected"],
            worst,
        )

    @task
    def quality_passed() -> None:
        """No-op marker so the passing branch has somewhere to go."""
        log.info("data quality clean, nothing quarantined")

    @task(trigger_rule="none_failed_min_one_success")
    def refresh_aggregates() -> int:
        """Recompute daily PnL from validated rows only.

        Reads ``v_valid_trade``, so whatever was quarantined is excluded by
        construction rather than by remembering to filter here.
        """
        from tradepnl.pnl import compute_daily_pnl

        result = compute_daily_pnl(_engine())
        log.info("aggregation: %s", result.summary())
        return result.rows_written

    @task(trigger_rule="none_failed_min_one_success")
    def record_outcome(outcome: dict[str, Any]) -> None:
        """Close out the validation_run row once the aggregate has been refreshed."""
        from tradedq.runs import finish_run
        from tradedq.validate import ValidationOutcome

        finish_run(
            _engine(),
            ValidationOutcome(
                run_id=uuid.UUID(outcome["run_id"]),
                success=outcome["success"],
                rows_checked=outcome["rows_checked"],
                failed_rules=outcome["failed_rules"],
                rules_total=outcome["rules_total"],
                details=outcome["details"],
            ),
        )

    schema = ensure_schema()
    loaded = load_feed()
    outcome = validate()
    branch = choose_path(outcome)
    quarantined = quarantine_rejects(outcome)
    alerted = alert(quarantined)
    passed = quality_passed()
    aggregates = refresh_aggregates()
    recorded = record_outcome(outcome)

    schema >> loaded >> outcome >> branch
    branch >> [quarantined, passed]
    quarantined >> alerted
    [alerted, passed] >> aggregates >> recorded


trade_data_quality()
