"""The data quality rules, defined once.

The obvious way to build this project is to write a Great Expectations suite and,
separately, some SQL that moves bad rows into a quarantine table. That works right up
until the two definitions disagree, at which point the report says the data is fine
and the quarantine table says otherwise, and nobody can tell which is lying.

So each rule is declared once here, carrying both representations:

* ``expectation`` is what Great Expectations asserts, and is what appears in the data
  docs and the audit trail.
* ``failure_predicate`` is SQL that is true for a row that breaks the rule, and is
  what the quarantine step uses to find and label offenders.

They are still two expressions of the same idea rather than one, which is the honest
cost of using a validation framework whose output is a report rather than a row set.
Keeping them adjacent, in one list, is what stops them drifting apart.
"""

from __future__ import annotations

from dataclasses import dataclass

import great_expectations.expectations as gxe
from great_expectations.expectations.expectation import Expectation

TRADE_COLUMNS = (
    "trade_id",
    "instrument_id",
    "side",
    "quantity",
    "price",
    "executed_at",
    "source_file",
    "ingested_at",
)


@dataclass(frozen=True)
class Rule:
    """One data quality rule.

    ``failure_predicate`` is None for rules that are not row-level -- a table's column
    set or the freshness of its newest row cannot be attributed to an individual row,
    so there is nothing sensible to quarantine.
    """

    name: str
    category: str
    description: str
    expectation: Expectation
    failure_predicate: str | None = None

    @property
    def quarantinable(self) -> bool:
        """Whether an offending row can be identified and moved."""
        return self.failure_predicate is not None


def build_rules(*, freshness_days: int = 400) -> list[Rule]:
    """Every rule applied to the trade table.

    ``freshness_days`` is generous because the synthetic feed spans a year. A real
    daily feed would use something like 2.
    """
    return [
        # --- Uniqueness -----------------------------------------------------------
        Rule(
            name="trade_id_unique",
            category="uniqueness",
            description=(
                "trade_id identifies exactly one execution. This is enforced by the "
                "primary key, so the expectation is really asserting that the loader "
                "collapsed the producer's retries rather than dropping the batch."
            ),
            expectation=gxe.ExpectColumnValuesToBeUnique(column="trade_id"),
        ),
        # --- Completeness ---------------------------------------------------------
        Rule(
            name="instrument_id_present",
            category="completeness",
            description="A trade with no instrument cannot be attributed to a position.",
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="instrument_id"),
            failure_predicate="t.instrument_id IS NULL",
        ),
        Rule(
            name="price_present",
            category="completeness",
            description="Price drives every PnL figure downstream.",
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="price"),
            failure_predicate="t.price IS NULL",
        ),
        Rule(
            name="quantity_present",
            category="completeness",
            description="Quantity drives position, so a null silently understates exposure.",
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="quantity"),
            failure_predicate="t.quantity IS NULL",
        ),
        Rule(
            name="executed_at_present",
            category="completeness",
            description=(
                "Without a timestamp a trade cannot be placed on a day, so it can be "
                "neither aggregated nor positioned against the load watermark."
            ),
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="executed_at"),
            failure_predicate="t.executed_at IS NULL",
        ),
        # --- Value ranges ---------------------------------------------------------
        Rule(
            name="quantity_positive",
            category="value_range",
            description=(
                "Direction is carried by side, not by the sign of quantity. A negative "
                "quantity means an upstream system is encoding direction twice, which "
                "would double the position if it were let through."
            ),
            expectation=gxe.ExpectColumnValuesToBeBetween(
                column="quantity", min_value=0, strict_min=True
            ),
            failure_predicate="t.quantity IS NOT NULL AND t.quantity <= 0",
        ),
        Rule(
            name="price_positive",
            category="value_range",
            description="A zero or negative fill price is a bad message, not a bargain.",
            expectation=gxe.ExpectColumnValuesToBeBetween(
                column="price", min_value=0, strict_min=True
            ),
            failure_predicate="t.price IS NOT NULL AND t.price <= 0",
        ),
        # Side is checked by two rules rather than one, and the reason is worth knowing.
        #
        # Great Expectations skips nulls in ExpectColumnValuesToBeInSet, as it does in
        # most column expectations. A single rule whose SQL predicate caught nulls but
        # whose expectation ignored them would report five bad rows while quarantining
        # ten -- which is precisely the drift this module is arranged to prevent, and
        # it happened here before the two counts were compared. Splitting the concern
        # makes both representations agree.
        Rule(
            name="side_present",
            category="completeness",
            description=(
                "Side must be populated. An empty string from the feed lands as NULL, "
                "which value-set checks skip, so it needs a rule of its own."
            ),
            expectation=gxe.ExpectColumnValuesToNotBeNull(column="side"),
            failure_predicate="t.side IS NULL",
        ),
        Rule(
            name="side_recognised",
            category="value_range",
            description=(
                "Venues disagree: some send BUY/SELL, some send B/S. Normalising is a "
                "mapping decision, so an unrecognised value is quarantined rather than "
                "guessed at."
            ),
            expectation=gxe.ExpectColumnValuesToBeInSet(column="side", value_set=["BUY", "SELL"]),
            failure_predicate="t.side IS NOT NULL AND upper(t.side) NOT IN ('BUY', 'SELL')",
        ),
        # --- Referential integrity ------------------------------------------------
        #
        # There is no built-in expectation for a cross-table relationship, so this is
        # asserted as "the orphan query returns no rows". That is the standard way to
        # express referential integrity in Great Expectations, and it is also why the
        # trade table has no foreign key: a constraint would abort the batch, whereas
        # this reports the problem and lets the good rows through.
        Rule(
            name="instrument_exists",
            category="referential_integrity",
            description=(
                "Every instrument_id must exist in the dimension. Reference data often "
                "lags the booking system, so this fires on real feeds."
            ),
            expectation=gxe.ExpectTableRowCountToEqual(value=0),
            failure_predicate=(
                "t.instrument_id IS NOT NULL AND NOT EXISTS ("
                "SELECT 1 FROM instrument i WHERE i.instrument_id = t.instrument_id)"
            ),
        ),
        # --- Freshness ------------------------------------------------------------
        Rule(
            name="feed_is_fresh",
            category="freshness",
            description=(
                f"The newest trade is within {freshness_days} days. A feed that has "
                "silently stopped looks identical to a quiet market unless something "
                "checks."
            ),
            expectation=gxe.ExpectColumnValuesToBeBetween(
                column="days_since_latest_trade", min_value=0, max_value=freshness_days
            ),
        ),
        # --- Schema drift ---------------------------------------------------------
        Rule(
            name="schema_unchanged",
            category="schema_drift",
            description=(
                "The column set is exactly what the pipeline was written against. A "
                "column added, removed or renamed upstream would otherwise change "
                "downstream results silently rather than failing the run."
            ),
            expectation=gxe.ExpectTableColumnsToMatchSet(column_set=list(TRADE_COLUMNS)),
        ),
    ]


def quarantinable_rules(rules: list[Rule]) -> list[Rule]:
    """The subset of rules that can be attributed to an individual row."""
    return [rule for rule in rules if rule.quarantinable]
