"""End-to-end validation runs against Great Expectations.

These are slower than the quarantine tests because they stand up a real Great
Expectations context, so there are deliberately few of them: enough to prove the
suite maps back to rules correctly and that clean and dirty data are distinguished.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import Engine, text

from tests.conftest import load, recent, row
from tradedq.quarantine import quarantine_failing_rows
from tradedq.rules import build_rules
from tradedq.runs import finish_run, recent_runs, start_run
from tradedq.validate import run_validation


@pytest.fixture()
def gx_root(tmp_path: Path) -> Path:
    """A throwaway Great Expectations project per test."""
    return tmp_path


def test_clean_data_passes_every_rule(clean_db: Engine, url: str, gx_root: Path) -> None:
    load(clean_db, [row("G1"), row("G2"), row("G3")])

    outcome = run_validation(clean_db, url, project_root=gx_root, build_docs=False)

    assert outcome.success, outcome.summary()
    assert outcome.failed_rules == []
    assert outcome.rows_checked == 3


def test_dirty_data_fails_the_specific_rules_it_breaks(
    clean_db: Engine, url: str, gx_root: Path
) -> None:
    load(
        clean_db,
        [
            row("G1"),
            row("NULLPRICE", price=""),
            row("NEGQTY", quantity=-3),
            row("ORPHAN", instrument_id=4242),
        ],
    )

    outcome = run_validation(clean_db, url, project_root=gx_root, build_docs=False)

    assert not outcome.success
    assert set(outcome.failed_rules) == {
        "price_present",
        "quantity_positive",
        "instrument_exists",
    }


def test_expectations_and_quarantine_agree_on_the_same_data(
    clean_db: Engine, url: str, gx_root: Path
) -> None:
    """Every row-level rule must report and quarantine the same set of problems.

    This is the test that justifies the layout of ``rules.py``, and it earns its keep:
    it caught a case where a single side rule quarantined ten rows but reported five,
    because Great Expectations skips nulls in a value-set check and the SQL predicate
    did not. Without this, the report and the quarantine table would have quietly
    disagreed.
    """
    load(
        clean_db,
        [
            row("CLEAN"),
            row("NO_PRICE", price=""),
            row("NO_SIDE", side=""),
            row("BAD_SIDE", side="X"),
            row("NEG_QTY", quantity=-1),
            row("ORPHAN", instrument_id=777),
            row("NO_TIME", executed_at=""),
        ],
    )

    outcome = run_validation(clean_db, url, project_root=gx_root, build_docs=False)
    result = quarantine_failing_rows(clean_db, build_rules(), run_id=outcome.run_id)

    quarantinable = {rule.name for rule in build_rules() if rule.quarantinable}
    reported = set(outcome.failed_rules) & quarantinable

    assert reported == set(result.reason_counts)


def test_schema_drift_is_detected(clean_db: Engine, url: str, gx_root: Path) -> None:
    """A column appearing upstream must fail the run rather than pass unnoticed."""
    load(clean_db, [row("G1")])
    with clean_db.begin() as conn:
        conn.execute(text("ALTER TABLE trade ADD COLUMN venue varchar(16)"))
    try:
        outcome = run_validation(clean_db, url, project_root=gx_root, build_docs=False)
        assert "schema_unchanged" in outcome.failed_rules
    finally:
        with clean_db.begin() as conn:
            conn.execute(text("ALTER TABLE trade DROP COLUMN venue"))


def test_a_stale_feed_fails_freshness(clean_db: Engine, url: str, gx_root: Path) -> None:
    """A feed that silently stopped looks like a quiet market unless something checks."""
    load(clean_db, [row("OLD", executed_at=recent(days_ago=900))])

    outcome = run_validation(clean_db, url, project_root=gx_root, build_docs=False)

    assert "feed_is_fresh" in outcome.failed_rules


def test_data_docs_are_written_when_requested(
    clean_db: Engine, url: str, gx_root: Path
) -> None:
    """The HTML report is the most convincing artefact this project produces."""
    load(clean_db, [row("G1"), row("BAD", price="")])

    outcome = run_validation(clean_db, url, project_root=gx_root, build_docs=True)

    index = gx_root / "gx" / "uncommitted" / "data_docs" / "local_site" / "index.html"
    assert index.exists()
    assert outcome.data_docs_url is not None


def test_a_run_is_recorded_with_its_rejection_breakdown(
    clean_db: Engine, url: str, gx_root: Path
) -> None:
    load(clean_db, [row("G1"), row("BAD", price="")])

    outcome = run_validation(clean_db, url, project_root=gx_root, build_docs=False)
    start_run(clean_db, outcome.run_id)
    result = quarantine_failing_rows(clean_db, build_rules(), run_id=outcome.run_id)
    finish_run(clean_db, outcome, result)

    history = recent_runs(clean_db, limit=1)
    assert len(history) == 1
    assert history[0].status == "failed"
    assert history[0].rows_rejected == 1
    assert history[0].rows_checked == 2


def test_the_rejection_count_is_recovered_when_the_caller_has_no_result(
    clean_db: Engine, url: str, gx_root: Path
) -> None:
    """The DAG skips the quarantine task when data is clean, so it has nothing to pass.

    Reading the count back from the table keeps the run record honest either way.
    """
    load(clean_db, [row("G1"), row("BAD", price=""), row("WORSE", quantity=-1)])

    outcome = run_validation(clean_db, url, project_root=gx_root, build_docs=False)
    quarantine_failing_rows(clean_db, build_rules(), run_id=outcome.run_id)
    finish_run(clean_db, outcome)  # deliberately without the quarantine result

    assert recent_runs(clean_db, limit=1)[0].rows_rejected == 2
