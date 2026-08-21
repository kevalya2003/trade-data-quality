"""Run the rules through Great Expectations and report the outcome.

Great Expectations is used here for what it is genuinely good at: producing a legible,
shareable record of what was checked and what failed. It is not used to decide which
rows to drop -- that is the quarantine step's job, driven by the same rule list, because
a validation framework reports on a batch rather than handing back the offending rows.

Three assets are validated rather than one, because the rules are not all the same
shape. Column-level rules run against the table. Referential integrity has no built-in
expectation, so it becomes "this orphan query returns no rows". Freshness is a property
of the newest row, so it becomes a one-row query asserting an age.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import great_expectations as gx
from great_expectations.checkpoint import UpdateDataDocsAction
from great_expectations.data_context.types.base import ProgressBarsConfig
from sqlalchemy import Engine, text

from tradedq.rules import Rule, build_rules

DATASOURCE_NAME = "trades_postgres"
CHECKPOINT_NAME = "trade_quality"

ORPHAN_QUERY = """
SELECT t.trade_id, t.instrument_id
FROM trade t
WHERE t.instrument_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM instrument i WHERE i.instrument_id = t.instrument_id)
"""

FRESHNESS_QUERY = """
SELECT COALESCE(now()::date - max(executed_at)::date, 99999) AS days_since_latest_trade
FROM trade
"""


@dataclass
class ValidationOutcome:
    """Result of one validation run, in terms of rules rather than expectations."""

    run_id: uuid.UUID
    success: bool
    rows_checked: int
    failed_rules: list[str] = field(default_factory=list)
    rules_total: int = 0
    details: dict[str, Any] = field(default_factory=dict)
    data_docs_url: str | None = None

    @property
    def rules_failed(self) -> int:
        """How many distinct rules failed."""
        return len(self.failed_rules)

    def summary(self) -> str:
        """One-line human summary."""
        if self.success:
            return f"all {self.rules_total} rules passed over {self.rows_checked} rows"
        return (
            f"{self.rules_failed} of {self.rules_total} rules failed over "
            f"{self.rows_checked} rows: {', '.join(sorted(self.failed_rules))}"
        )


def _tag(rule: Rule):  # noqa: ANN202 - returns a GX Expectation
    """Attach the rule's identity to its expectation so results map back to it."""
    expectation = rule.expectation
    expectation.meta = {
        "rule": rule.name,
        "category": rule.category,
        "description": rule.description,
    }
    return expectation


def run_validation(
    engine: Engine,
    connection_string: str,
    *,
    rules: list[Rule] | None = None,
    project_root: Path | None = None,
    run_id: uuid.UUID | None = None,
    build_docs: bool = True,
) -> ValidationOutcome:
    """Validate the trade table and return a rule-level outcome.

    ``project_root`` is the directory Great Expectations creates its ``gx/`` folder
    inside. A file-backed context rather than an ephemeral one, because the HTML data
    docs are the most useful thing this project produces and an ephemeral context
    throws them away.
    """
    rules = rules or build_rules()
    run_id = run_id or uuid.uuid4()
    project_root = project_root or Path(".")
    project_root.mkdir(parents=True, exist_ok=True)

    context = gx.get_context(mode="file", project_root_dir=str(project_root))
    # The metric progress bars write to stderr on every run, which buries the one line
    # anybody actually wants. Off by default; the run is fast enough not to need them.
    context.variables.progress_bars = ProgressBarsConfig(
        globally=False, metric_calculations=False
    )

    datasource = context.data_sources.add_or_update_postgres(
        name=DATASOURCE_NAME, connection_string=connection_string
    )

    by_name = {rule.name: rule for rule in rules}
    column_rules = [
        rule
        for rule in rules
        if rule.name not in {"instrument_exists", "feed_is_fresh"}
    ]

    definitions = [
        _definition(
            context,
            datasource,
            asset_name="trade_table",
            suite_name="trade_columns",
            expectations=[_tag(rule) for rule in column_rules],
            table_name="trade",
        ),
        _definition(
            context,
            datasource,
            asset_name="orphan_trades",
            suite_name="referential_integrity",
            expectations=[_tag(by_name["instrument_exists"])],
            query=ORPHAN_QUERY,
        ),
        _definition(
            context,
            datasource,
            asset_name="feed_freshness",
            suite_name="freshness",
            expectations=[_tag(by_name["feed_is_fresh"])],
            query=FRESHNESS_QUERY,
        ),
    ]

    actions = [UpdateDataDocsAction(name="update_data_docs")] if build_docs else []
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=CHECKPOINT_NAME,
            validation_definitions=definitions,
            actions=actions,
            result_format="SUMMARY",
        )
    )

    result = checkpoint.run()

    failed: list[str] = []
    details: dict[str, Any] = {}
    for identifier, validation_result in result.run_results.items():
        for expectation_result in validation_result.results:
            meta = expectation_result.expectation_config.meta or {}
            rule_name = meta.get("rule", str(identifier))
            details[rule_name] = {
                "success": expectation_result.success,
                "category": meta.get("category"),
                "observed": _observed(expectation_result),
            }
            if not expectation_result.success:
                failed.append(rule_name)

    with engine.connect() as conn:
        rows_checked = conn.execute(text("SELECT count(*) FROM trade")).scalar_one()

    return ValidationOutcome(
        run_id=run_id,
        success=bool(result.success),
        rows_checked=rows_checked,
        failed_rules=sorted(set(failed)),
        rules_total=len(rules),
        details=details,
        data_docs_url=_first_docs_url(context) if build_docs else None,
    )


def _definition(
    context,  # noqa: ANN001 - GX AbstractDataContext
    datasource,  # noqa: ANN001 - GX SQLDatasource
    *,
    asset_name: str,
    suite_name: str,
    expectations: list,
    table_name: str | None = None,
    query: str | None = None,
):  # noqa: ANN202 - GX ValidationDefinition
    """Wire one asset to one suite. Idempotent, so a re-run does not duplicate config."""
    try:
        asset = datasource.get_asset(asset_name)
    except LookupError:
        asset = (
            datasource.add_table_asset(name=asset_name, table_name=table_name)
            if table_name
            else datasource.add_query_asset(name=asset_name, query=query)
        )

    try:
        batch_definition = asset.get_batch_definition(f"{asset_name}_whole")
    except KeyError:
        batch_definition = asset.add_batch_definition_whole_table(f"{asset_name}_whole")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name=suite_name))
    suite.expectations = expectations
    suite.save()

    return context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name=f"{suite_name}_definition", data=batch_definition, suite=suite
        )
    )


def _observed(expectation_result) -> Any:  # noqa: ANN001 - GX ExpectationValidationResult
    """Pull the interesting number out of a result, whatever shape it is."""
    observed = (expectation_result.result or {}).get("observed_value")
    if observed is None:
        observed = (expectation_result.result or {}).get("unexpected_count")
    return observed


def _first_docs_url(context) -> str | None:  # noqa: ANN001 - GX AbstractDataContext
    """Local path to the generated data docs, if any were built."""
    try:
        sites = context.get_docs_sites_urls()
    except Exception:  # noqa: BLE001 - docs are a nicety, never fail the run for them
        return None
    return sites[0]["site_url"] if sites else None
