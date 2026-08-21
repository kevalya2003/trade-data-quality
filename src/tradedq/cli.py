"""Command line entry points for the data quality stage.

Everything the Airflow DAG does is available here too, so the pipeline can be run and
debugged without a scheduler in the way.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import click
from sqlalchemy import create_engine

from tradedq.quarantine import quarantine_failing_rows
from tradedq.rules import build_rules
from tradedq.runs import finish_run, recent_runs, start_run
from tradedq.schema import create_schema, drop_schema
from tradedq.validate import run_validation

# Port 55433 is this project's own compose stack. The pipeline project's stack uses
# 55432, so both can be up at once and neither project's tests disturb the other's data.
DEFAULT_DATABASE_URL = "postgresql+psycopg://trades:trades@localhost:55433/trades"


def resolve_url(override: str | None) -> str:
    """Connection string from the flag, the environment, or the local default."""
    return override or os.environ.get("TRADEDQ_DATABASE_URL", DEFAULT_DATABASE_URL)


@click.group()
@click.option("--database-url", "db_url", default=None, help="Override TRADEDQ_DATABASE_URL.")
@click.pass_context
def main(ctx: click.Context, db_url: str | None) -> None:
    """Data quality validation for the trade pipeline."""
    ctx.ensure_object(dict)
    ctx.obj["url"] = resolve_url(db_url)
    ctx.obj["engine"] = create_engine(ctx.obj["url"], pool_pre_ping=True, future=True)


@main.command("init-db")
@click.pass_context
def init_db(ctx: click.Context) -> None:
    """Create rejected_trades and validation_run."""
    create_schema(ctx.obj["engine"])
    click.echo("data quality schema ready")


@main.command("reset")
@click.confirmation_option(prompt="Drop the quality tables?")
@click.pass_context
def reset(ctx: click.Context) -> None:
    """Drop the quality tables."""
    drop_schema(ctx.obj["engine"])
    click.echo("data quality schema dropped")


@main.command("rules")
def rules_command() -> None:
    """List the rules and what each one is for."""
    for rule in build_rules():
        marker = "row" if rule.quarantinable else "tbl"
        click.echo(f"[{marker}] {rule.category:<22} {rule.name}")
        click.echo(f"      {rule.description}")


@main.command("validate")
@click.option(
    "--project-dir",
    type=click.Path(path_type=Path),
    default=Path("."),
    show_default=True,
    help="Directory Great Expectations keeps its gx/ folder in.",
)
@click.option("--no-docs", is_flag=True, help="Skip building the HTML data docs.")
@click.option("--quarantine/--no-quarantine", default=True, show_default=True)
@click.pass_context
def validate(ctx: click.Context, project_dir: Path, no_docs: bool, quarantine: bool) -> None:
    """Run the rules, record the run, and quarantine offending rows."""
    engine = ctx.obj["engine"]
    run_id = uuid.uuid4()

    create_schema(engine)
    start_run(engine, run_id)

    outcome = run_validation(
        engine,
        ctx.obj["url"],
        project_root=project_dir,
        run_id=run_id,
        build_docs=not no_docs,
    )
    click.echo(outcome.summary())

    result = None
    if quarantine:
        result = quarantine_failing_rows(engine, build_rules(), run_id=run_id)
        click.echo(result.summary())

    finish_run(engine, outcome, result)

    if outcome.data_docs_url:
        click.echo(f"data docs: {outcome.data_docs_url}")

    # A non-zero exit lets a scheduler or a CI job treat bad data as a failure without
    # having to parse stdout.
    ctx.exit(0 if outcome.success else 1)


@main.command("history")
@click.option("--limit", default=10, show_default=True)
@click.pass_context
def history(ctx: click.Context, limit: int) -> None:
    """Show recent validation runs, newest first."""
    for record in recent_runs(ctx.obj["engine"], limit):
        click.echo(
            f"{record.run_id}  {record.status:<7} "
            f"checked={record.rows_checked:<7} rejected={record.rows_rejected:<5} "
            f"rules_failed={record.rules_failed}"
        )


if __name__ == "__main__":
    main()
