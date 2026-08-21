"""Move failing rows aside instead of failing the batch.

The argument for quarantining: a single bad record should not cost you a day of data.
If one row out of fifty thousand has a null price, aborting the load means nobody gets
yesterday's PnL because of a typo in one booking.

The argument against, which is real and you should be able to make it: silently
continuing with five per cent of rows missing can be worse than failing loudly,
because a downstream consumer cannot distinguish "no trades in that instrument" from
"trades we threw away". Aggregates quietly understate and nobody notices for a month.

Which is right depends on whether consumers can tolerate gaps. The position taken here
is that they can, *provided* the gap is auditable -- hence a quarantine table that
records the row, every rule it broke and the run that rejected it, rather than a log
line that scrolls away.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import Engine, text

from tradedq.rules import Rule, quarantinable_rules


@dataclass(frozen=True)
class QuarantineResult:
    """What a quarantine pass moved."""

    run_id: uuid.UUID
    rows_rejected: int
    reason_counts: dict[str, int]

    def summary(self) -> str:
        """One-line human summary."""
        if not self.rows_rejected:
            return "no rows quarantined"
        breakdown = ", ".join(
            f"{reason}={count}" for reason, count in sorted(self.reason_counts.items())
        )
        return f"quarantined {self.rows_rejected} rows ({breakdown})"


def build_quarantine_sql(rules: list[Rule]) -> str:
    """Generate the quarantine statement from the rule list.

    Generating rather than hand-writing is the point: adding a rule to ``rules.py``
    automatically extends both the expectation suite and this statement, so the two
    cannot fall out of step.
    """
    applicable = quarantinable_rules(rules)
    if not applicable:
        raise ValueError("no quarantinable rules")

    reason_cases = ",\n            ".join(
        f"CASE WHEN {rule.failure_predicate} THEN '{rule.name}' END" for rule in applicable
    )
    any_failure = "\n           OR ".join(f"({rule.failure_predicate})" for rule in applicable)

    return f"""
    INSERT INTO rejected_trades (
        trade_id, instrument_id, side, quantity, price, executed_at, source_file,
        reasons, run_id
    )
    SELECT
        t.trade_id,
        t.instrument_id,
        t.side,
        t.quantity,
        t.price,
        t.executed_at,
        t.source_file,
        array_remove(ARRAY[
            {reason_cases}
        ], NULL) AS reasons,
        CAST(:run_id AS uuid)
    FROM trade t
    WHERE {any_failure}
    ON CONFLICT (trade_id) DO UPDATE SET
        instrument_id = EXCLUDED.instrument_id,
        side          = EXCLUDED.side,
        quantity      = EXCLUDED.quantity,
        price         = EXCLUDED.price,
        executed_at   = EXCLUDED.executed_at,
        source_file   = EXCLUDED.source_file,
        reasons       = EXCLUDED.reasons,
        run_id        = EXCLUDED.run_id,
        rejected_at   = now()
    """


def quarantine_failing_rows(
    engine: Engine,
    rules: list[Rule],
    run_id: uuid.UUID | None = None,
) -> QuarantineResult:
    """Copy every rule-breaking row into ``rejected_trades`` with its reasons.

    Rows are copied, not deleted. The trade table stays a faithful record of what the
    source actually sent; ``v_valid_trade`` is what excludes bad rows from aggregation.
    Deleting them would destroy the evidence needed to go back to the upstream team.
    """
    run_id = run_id or uuid.uuid4()
    statement = text(build_quarantine_sql(rules))

    with engine.begin() as conn:
        conn.execute(statement, {"run_id": str(run_id)})
        rows = conn.execute(
            text(
                """
                SELECT unnest(reasons) AS reason, count(*) AS n
                FROM rejected_trades
                WHERE run_id = CAST(:run_id AS uuid)
                GROUP BY 1
                """
            ),
            {"run_id": str(run_id)},
        ).all()
        total = conn.execute(
            text("SELECT count(*) FROM rejected_trades WHERE run_id = CAST(:run_id AS uuid)"),
            {"run_id": str(run_id)},
        ).scalar_one()

    return QuarantineResult(
        run_id=run_id,
        rows_rejected=total,
        reason_counts={row.reason: row.n for row in rows},
    )
