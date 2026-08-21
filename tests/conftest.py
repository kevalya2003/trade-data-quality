"""Fixtures for the data quality tests.

These tests need the pipeline project's schema, because validating the trade table
requires there to be a trade table. That is a real dependency rather than an awkward
one: this project exists to check the other project's output, and a test suite that
invented its own copy of the schema would stop catching the case where the two drift
apart.
"""

from __future__ import annotations

import datetime as dt
import os

import pytest
from sqlalchemy import Engine, create_engine, text

from tradedq.schema import create_schema, drop_schema

pytest.importorskip(
    "tradepnl",
    reason="Install the pipeline project first: pip install -e ../trade-pnl-pipeline",
)

from tradepnl.db import create_schema as create_pipeline_schema  # noqa: E402
from tradepnl.db import drop_schema as drop_pipeline_schema  # noqa: E402
from tradepnl.generate import TradeRow  # noqa: E402
from tradepnl.load import load_trades  # noqa: E402

TEST_URL = os.environ.get(
    "TRADEDQ_TEST_DATABASE_URL",
    os.environ.get(
        "TRADEDQ_DATABASE_URL",
        "postgresql+psycopg://trades:trades@localhost:55433/trades",
    ),
)

REQUIRE_DB = os.environ.get("TRADEDQ_REQUIRE_DB") == "1"


@pytest.fixture(scope="session")
def url() -> str:
    """Connection string, needed separately because Great Expectations wants one."""
    return TEST_URL


@pytest.fixture(scope="session")
def engine(url: str) -> Engine:
    """Session engine with both projects' schemas present."""
    eng = create_engine(url, pool_pre_ping=True, future=True)
    try:
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        message = f"PostgreSQL not reachable at {url}: {exc}"
        if REQUIRE_DB:
            pytest.fail(message)
        pytest.skip(f"{message}\nStart one with: docker compose up -d postgres")

    drop_schema(eng)
    drop_pipeline_schema(eng)
    create_pipeline_schema(eng)
    create_schema(eng)
    yield eng
    eng.dispose()


@pytest.fixture()
def clean_db(engine: Engine) -> Engine:
    """Empty everything before each test."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE rejected_trades, validation_run, trade, daily_pnl, "
                "etl_watermark, instrument CASCADE"
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO instrument (instrument_id, symbol, asset_class, currency)
                VALUES (1, 'AAPL', 'EQUITY', 'USD'), (2, 'MSFT', 'EQUITY', 'USD')
                """
            )
        )
    return engine


def recent(days_ago: int = 1, hour: int = 10) -> str:
    """A timestamp relative to now, so freshness checks behave in tests."""
    moment = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days_ago)
    return moment.replace(hour=hour, minute=0, second=0, microsecond=0).isoformat()


def row(
    trade_id: str,
    *,
    instrument_id: int | str = 1,
    side: str = "BUY",
    quantity: str | int = 100,
    price: str = "10.00",
    executed_at: str | None = None,
) -> TradeRow:
    """Build one feed row."""
    return TradeRow(
        trade_id=trade_id,
        instrument_id=str(instrument_id),
        side=side,
        quantity=str(quantity),
        price=price,
        executed_at=executed_at if executed_at is not None else recent(),
    )


def load(engine: Engine, rows: list[TradeRow]) -> None:
    """Land rows in the trade table."""
    load_trades(engine, rows, incremental=False)


def reasons_for(engine: Engine, trade_id: str) -> list[str]:
    """The quarantine reasons recorded against one trade."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT reasons FROM rejected_trades WHERE trade_id = :tid"),
            {"tid": trade_id},
        ).scalar_one_or_none()
    return sorted(result) if result else []
