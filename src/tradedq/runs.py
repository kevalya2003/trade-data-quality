"""Persist the history of validation runs.

Data docs are for a human reading one run. This table is for the question a human
asks next: is this getting worse? A run row per execution makes rejection rates
trendable, which is how you notice an upstream system slowly degrading rather than
breaking outright.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from sqlalchemy import Engine, text

from tradedq.quarantine import QuarantineResult
from tradedq.validate import ValidationOutcome


@dataclass(frozen=True)
class RunRecord:
    """A row of ``validation_run``, for reporting."""

    run_id: uuid.UUID
    status: str
    rows_checked: int
    rows_rejected: int
    rules_failed: int


def start_run(engine: Engine, run_id: uuid.UUID) -> None:
    """Record that a run has begun, so a crashed run is visible as ``running``."""
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO validation_run (run_id, status)
                VALUES (CAST(:run_id AS uuid), 'running')
                ON CONFLICT (run_id) DO NOTHING
                """
            ),
            {"run_id": str(run_id)},
        )


def _quarantine_from_table(engine: Engine, run_id: uuid.UUID) -> tuple[int, dict[str, int]]:
    """Read what a run actually quarantined, straight from the table.

    The caller does not always have the quarantine result to hand -- in the Airflow
    DAG the quarantine task is skipped entirely when validation passes, so there is no
    value to pass along. Reading back from the table keeps one source of truth and
    avoids the run record disagreeing with the rows it describes.
    """
    with engine.connect() as conn:
        total = conn.execute(
            text("SELECT count(*) FROM rejected_trades WHERE run_id = CAST(:rid AS uuid)"),
            {"rid": str(run_id)},
        ).scalar_one()
        rows = conn.execute(
            text(
                """
                SELECT unnest(reasons) AS reason, count(*) AS n
                FROM rejected_trades
                WHERE run_id = CAST(:rid AS uuid)
                GROUP BY 1
                """
            ),
            {"rid": str(run_id)},
        ).all()
    return total, {row.reason: row.n for row in rows}


def finish_run(
    engine: Engine,
    outcome: ValidationOutcome,
    quarantine: QuarantineResult | None = None,
) -> None:
    """Close out a run with its result."""
    if quarantine is not None:
        rows_rejected = quarantine.rows_rejected
        reason_counts = quarantine.reason_counts
    else:
        rows_rejected, reason_counts = _quarantine_from_table(engine, outcome.run_id)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO validation_run (
                    run_id, status, rows_checked, rows_rejected,
                    rules_total, rules_failed, details, finished_at
                )
                VALUES (
                    CAST(:run_id AS uuid), :status, :rows_checked, :rows_rejected,
                    :rules_total, :rules_failed, CAST(:details AS jsonb), now()
                )
                ON CONFLICT (run_id) DO UPDATE SET
                    status        = EXCLUDED.status,
                    rows_checked  = EXCLUDED.rows_checked,
                    rows_rejected = EXCLUDED.rows_rejected,
                    rules_total   = EXCLUDED.rules_total,
                    rules_failed  = EXCLUDED.rules_failed,
                    details       = EXCLUDED.details,
                    finished_at   = EXCLUDED.finished_at
                """
            ),
            {
                "run_id": str(outcome.run_id),
                "status": "passed" if outcome.success else "failed",
                "rows_checked": outcome.rows_checked,
                "rows_rejected": rows_rejected,
                "rules_total": outcome.rules_total,
                "rules_failed": outcome.rules_failed,
                "details": json.dumps(
                    {"rules": outcome.details, "reason_counts": reason_counts},
                    default=str,
                ),
            },
        )


def recent_runs(engine: Engine, limit: int = 10) -> list[RunRecord]:
    """Most recent runs, newest first."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT run_id, status, rows_checked, rows_rejected, rules_failed
                FROM validation_run
                ORDER BY started_at DESC
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).all()
    return [
        RunRecord(
            run_id=row.run_id,
            status=row.status,
            rows_checked=row.rows_checked,
            rows_rejected=row.rows_rejected,
            rules_failed=row.rules_failed,
        )
        for row in rows
    ]
