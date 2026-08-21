"""Tables owned by the data quality project.

The pipeline project owns ``trade``, ``instrument`` and ``daily_pnl``. This project
adds two tables of its own and never alters the others, which keeps the ownership
boundary obvious even though both share a database.
"""

from __future__ import annotations

from sqlalchemy import Engine, text

REJECTED_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS rejected_trades (
    trade_id      varchar(32) PRIMARY KEY,
    instrument_id integer,
    side          varchar(8),
    quantity      numeric(18, 4),
    price         numeric(18, 6),
    executed_at   timestamptz,
    source_file   varchar(255),
    -- An array rather than a single reason: a row with a null price and a negative
    -- quantity is broken in two ways, and reporting only the first would send
    -- somebody to fix half of it.
    reasons       text[]      NOT NULL,
    run_id        uuid        NOT NULL,
    rejected_at   timestamptz NOT NULL DEFAULT now()
);
"""

VALIDATION_RUN_DDL = """
CREATE TABLE IF NOT EXISTS validation_run (
    run_id         uuid PRIMARY KEY,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz,
    status         varchar(16) NOT NULL,
    rows_checked   bigint      NOT NULL DEFAULT 0,
    rows_rejected  bigint      NOT NULL DEFAULT 0,
    rules_total    integer     NOT NULL DEFAULT 0,
    rules_failed   integer     NOT NULL DEFAULT 0,
    details        jsonb       NOT NULL DEFAULT '{}'::jsonb
);
"""

INDEXES = (
    "CREATE INDEX IF NOT EXISTS ix_rejected_trades_run ON rejected_trades (run_id)",
    "CREATE INDEX IF NOT EXISTS ix_rejected_trades_reasons ON rejected_trades USING gin (reasons)",
    "CREATE INDEX IF NOT EXISTS ix_validation_run_started ON validation_run (started_at DESC)",
)


def create_schema(engine: Engine) -> None:
    """Create the quality tables. Safe to run repeatedly."""
    with engine.begin() as conn:
        conn.execute(text(REJECTED_TRADES_DDL))
        conn.execute(text(VALIDATION_RUN_DDL))
        for statement in INDEXES:
            conn.execute(text(statement))


def drop_schema(engine: Engine) -> None:
    """Drop the quality tables."""
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS rejected_trades"))
        conn.execute(text("DROP TABLE IF EXISTS validation_run"))
