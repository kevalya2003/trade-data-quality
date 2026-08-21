"""Quarantine behaviour.

The claim this project makes is that one bad row does not cost you the batch, and that
what was rejected stays auditable. These tests are that claim.
"""

from __future__ import annotations

from sqlalchemy import Engine, text

from tests.conftest import load, reasons_for, row
from tradedq.quarantine import build_quarantine_sql, quarantine_failing_rows
from tradedq.rules import build_rules, quarantinable_rules


def count(engine: Engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def test_good_rows_are_left_alone(clean_db: Engine) -> None:
    load(clean_db, [row("GOOD1"), row("GOOD2")])

    result = quarantine_failing_rows(clean_db, build_rules())

    assert result.rows_rejected == 0
    assert count(clean_db, "rejected_trades") == 0


def test_a_bad_row_is_quarantined_and_the_rest_of_the_batch_survives(clean_db: Engine) -> None:
    """The whole point: a single bad record must not cost a day of data."""
    load(clean_db, [row("GOOD1"), row("BAD", price=""), row("GOOD2")])

    result = quarantine_failing_rows(clean_db, build_rules())

    assert result.rows_rejected == 1
    assert reasons_for(clean_db, "BAD") == ["price_present"]
    # The good rows are still available to aggregate.
    assert count(clean_db, "v_valid_trade") == 2


def test_rows_are_copied_not_deleted(clean_db: Engine) -> None:
    """Deleting would destroy the evidence needed to go back to the upstream team."""
    load(clean_db, [row("BAD", price="")])

    quarantine_failing_rows(clean_db, build_rules())

    assert count(clean_db, "trade") == 1
    assert count(clean_db, "rejected_trades") == 1


def test_a_row_broken_several_ways_records_every_reason(clean_db: Engine) -> None:
    """Reporting only the first fault would send somebody to fix half the problem."""
    load(clean_db, [row("MULTI", price="", quantity=-5, side="X")])

    quarantine_failing_rows(clean_db, build_rules())

    assert reasons_for(clean_db, "MULTI") == [
        "price_present",
        "quantity_positive",
        "side_recognised",
    ]


def test_a_missing_side_is_completeness_not_an_unrecognised_value(clean_db: Engine) -> None:
    """An empty side arrives as NULL, which a value-set check skips.

    Separating the two keeps the reason accurate and keeps the expectation suite and
    this table reporting the same number.
    """
    load(clean_db, [row("EMPTY", side=""), row("WRONG", side="X")])

    quarantine_failing_rows(clean_db, build_rules())

    assert reasons_for(clean_db, "EMPTY") == ["side_present"]
    assert reasons_for(clean_db, "WRONG") == ["side_recognised"]


def test_orphan_instruments_are_caught_by_referential_integrity(clean_db: Engine) -> None:
    load(clean_db, [row("ORPHAN", instrument_id=9999)])

    quarantine_failing_rows(clean_db, build_rules())

    assert reasons_for(clean_db, "ORPHAN") == ["instrument_exists"]


def test_quarantining_twice_does_not_duplicate(clean_db: Engine) -> None:
    """Reruns happen. The upsert on trade_id means the table converges."""
    load(clean_db, [row("BAD", price="")])

    first = quarantine_failing_rows(clean_db, build_rules())
    second = quarantine_failing_rows(clean_db, build_rules())

    assert first.rows_rejected == 1
    assert second.rows_rejected == 1
    assert count(clean_db, "rejected_trades") == 1


def test_a_repaired_row_updates_its_quarantine_record(clean_db: Engine) -> None:
    """Re-quarantining reflects the current state rather than the original fault."""
    load(clean_db, [row("FIXME", price="", quantity=-1)])
    quarantine_failing_rows(clean_db, build_rules())
    assert reasons_for(clean_db, "FIXME") == ["price_present", "quantity_positive"]

    # Upstream resends with the price corrected but the quantity still wrong.
    load(clean_db, [row("FIXME", price="10.00", quantity=-1)])
    quarantine_failing_rows(clean_db, build_rules())

    assert reasons_for(clean_db, "FIXME") == ["quantity_positive"]


def test_reason_counts_are_reported_per_rule(clean_db: Engine) -> None:
    load(
        clean_db,
        [
            row("N1", price=""),
            row("N2", price=""),
            row("Q1", quantity=0),
            row("OK", price="12.00"),
        ],
    )

    result = quarantine_failing_rows(clean_db, build_rules())

    assert result.rows_rejected == 3
    assert result.reason_counts == {"price_present": 2, "quantity_positive": 1}


def test_generated_sql_covers_every_quarantinable_rule(clean_db: Engine) -> None:
    """Adding a rule must extend the quarantine statement without anyone editing SQL."""
    rules = build_rules()
    sql = build_quarantine_sql(rules)

    for rule in quarantinable_rules(rules):
        assert f"'{rule.name}'" in sql, f"{rule.name} missing from generated SQL"


def test_table_level_rules_are_not_quarantinable(clean_db: Engine) -> None:
    """Freshness and schema drift cannot be blamed on an individual row."""
    names = {rule.name for rule in build_rules() if not rule.quarantinable}
    assert names == {"feed_is_fresh", "schema_unchanged", "trade_id_unique"}
