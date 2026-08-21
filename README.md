# Trade Data Quality Pipeline

Validates the trade data loaded by the [pipeline project](../trade-pnl-pipeline), quarantines the rows
that fail with the reasons they failed, and only lets aggregation run against what
passed. Orchestrated as an Airflow DAG.

The premise: a single bad record should not cost you a day of data, and a row you
threw away should still be explainable a month later.

## What a run produces

```
$ tradedq validate
6 of 12 rules failed over 50000 rows: executed_at_present, instrument_exists,
  price_present, quantity_positive, side_present, side_recognised
quarantined 78 rows (executed_at_present=8, instrument_exists=15, price_present=25,
  quantity_positive=20, side_present=1, side_recognised=9)
data docs: file:///.../gx/uncommitted/data_docs/local_site/index.html
```

Exit status is non-zero when data fails, so a scheduler or CI job can treat bad data
as a failure without parsing stdout.

The HTML report Great Expectations generates alongside it:

![Data docs validation detail](docs/data-docs-detail.png)

## The DAG

```
ensure_schema -> load_feed -> validate -> choose_path
                                            |-- failed --> quarantine_rejects --> alert --.
                                            |-- passed --> quality_passed ----------------+
                                                                                          |
                                                                     refresh_aggregates <-'
                                                                              |
                                                                       record_outcome
```

The branch is the point rather than decoration. `refresh_aggregates` reads from
`v_valid_trade`, so aggregation only ever sees rows that passed; when validation
fails, the quarantine task runs first so that what was excluded is recorded before
anything downstream depends on its absence.

A real run over 20,000 generated trades:

| Task | State |
| --- | --- |
| `ensure_schema` | success |
| `load_feed` | success |
| `validate` | success |
| `choose_path` | success |
| `quality_passed` | **skipped** |
| `quarantine_rejects` | success |
| `alert` | success |
| `refresh_aggregates` | success |
| `record_outcome` | success |

`quality_passed` is skipped because the feed was dirty, which is the branch doing its
job. Aggregation still ran, over the 19,922 rows that survived.

## Running it

Requires Docker.

```bash
docker compose up -d          # PostgreSQL + Airflow 3.3 on http://localhost:18080
```

Then unpause and trigger `trade_data_quality` in the UI, or from the command line:

```bash
docker compose exec airflow airflow dags unpause trade_data_quality
docker compose exec airflow airflow dags trigger trade_data_quality
```

Without Airflow, which is faster when you are actually working on the rules:

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]" && .venv/bin/pip install -e ../trade-pnl-pipeline
export TRADEDQ_DATABASE_URL="postgresql+psycopg://trades:trades@localhost:55433/trades"

tradedq rules       # what is checked and why
tradedq validate    # run the rules, quarantine, write data docs
tradedq history     # rejection rates over recent runs
pytest              # 19 tests
```

## Design decisions

### Every rule is declared once

The obvious way to build this is a Great Expectations suite plus, separately, some SQL
that moves bad rows aside. That works right up until the two disagree, at which point
the report says the data is fine and the quarantine table says otherwise and nobody
can tell which is lying.

So `rules.py` declares each rule once, carrying both representations: the `expectation`
that Great Expectations asserts and appears in the report, and a `failure_predicate`
in SQL that is true for a row breaking the rule. The quarantine statement is
*generated* from that list, so adding a rule extends both without anyone editing SQL.

They are still two expressions of one idea, which is the honest cost of using a
framework whose output is a report rather than a row set. Keeping them adjacent is
what stops them drifting.

**This caught a real bug.** `ExpectColumnValuesToBeInSet` skips nulls, as most Great
Expectations column checks do. The loader turns an empty `side` into NULL. So a single
`side` rule quarantined ten rows while reporting five, and the two numbers only
disagreed by five out of fifty thousand — small enough to go unnoticed indefinitely.
The fix was to split it into `side_present` (a null check) and `side_recognised` (a
value check), which makes both representations agree.
`test_expectations_and_quarantine_agree_on_the_same_data` now fails if it happens again.

### Quarantine rather than abort — and the argument against

Aborting the batch means one typo in one booking costs everybody yesterday's PnL. So
failing rows are copied into `rejected_trades` and the rest of the batch proceeds.

The counter-argument is real and worth being able to make: silently continuing with
five per cent of rows missing can be worse than failing loudly, because a downstream
consumer cannot distinguish "no trades in that instrument" from "trades we threw
away". Aggregates quietly understate and nobody notices for a month.

Which is right depends on whether consumers can tolerate gaps. The position taken here
is that they can, *provided* the gap is auditable — hence a table recording the row,
every rule it broke, and the run that rejected it, rather than a log line that scrolls
away. A dataset feeding regulatory reporting would be a reasonable place to choose the
opposite.

### Rows are copied, not moved

`quarantine_failing_rows` does not delete from `trade`. The trade table stays a
faithful record of what the source actually sent, and `v_valid_trade` is what excludes
bad rows from aggregation. Deleting would destroy the evidence needed to go back to
the upstream team and say "you sent us these fifteen instrument IDs that do not exist".

### Reasons are an array

A row with a null price *and* a negative quantity is broken in two ways. Recording
only the first sends somebody to fix half the problem and be surprised when it fails
again.

### Referential integrity is a query, not a constraint

There is no built-in expectation for a cross-table relationship, so it is asserted as
"this orphan query returns no rows", which is the standard Great Expectations idiom.
It is also why the pipeline project's `trade` table has no foreign key on
`instrument_id`: a constraint would abort the batch, whereas this reports the problem
and lets the good rows through. Reference data lagging the booking system is normal,
not exceptional.

### Three assets, not one

The rules are not all the same shape. Column rules run against the table; referential
integrity runs against the orphan query; freshness runs against a one-row query
computing the age of the newest trade. Trying to force all three into one asset would
have meant giving up on two of them.

### The DAG is a thin wrapper

Every task is a few lines calling a function in `tradedq` or `tradepnl`. Business logic
inside an Airflow task can only be tested by running Airflow, which is slow enough
that in practice it does not get tested at all. Airflow is deliberately *not* a
dependency of the package: the validation logic is a library that happens to be
scheduled, and it could move to another orchestrator without a rewrite.

### `validate` does not raise on bad data

Bad data is an expected outcome, not an error. Raising in the validate task would mean
the branch never executes and the quarantine never happens — precisely the failure
mode this pipeline exists to avoid. The task succeeds and returns a verdict; the
branch decides what to do about it.

## The rules

| Rule | Category | Row-level | What it catches |
| --- | --- | --- | --- |
| `trade_id_unique` | uniqueness | no | The loader collapsing a producer's retries |
| `instrument_id_present` | completeness | yes | A trade that cannot be attributed to a position |
| `price_present` | completeness | yes | A missing field that drives every PnL figure |
| `quantity_present` | completeness | yes | A null that silently understates exposure |
| `executed_at_present` | completeness | yes | A trade that cannot be placed on a day |
| `side_present` | completeness | yes | An empty side, which value checks skip |
| `quantity_positive` | value range | yes | Direction encoded twice, doubling the position |
| `price_positive` | value range | yes | A bad message rather than a bargain |
| `side_recognised` | value range | yes | `B`/`S` from a venue that disagrees on encoding |
| `instrument_exists` | referential integrity | yes | Reference data lagging the booking system |
| `feed_is_fresh` | freshness | no | A feed that stopped, which looks like a quiet market |
| `schema_unchanged` | schema drift | no | A column added or renamed upstream |

`tradedq rules` prints this with the reasoning for each.

Freshness, schema drift and uniqueness are table-level: they cannot be blamed on an
individual row, so there is nothing sensible to quarantine. They fail the run without
moving anything.

## Tables this project owns

The pipeline project owns `trade`, `instrument` and `daily_pnl`. This project adds two
and never alters the others, so the ownership boundary stays obvious despite the
shared database.

- **`rejected_trades`** — the row as received, the array of rules it broke, and the run
  that rejected it. Upserted on `trade_id`, so re-running converges and a repaired row
  updates rather than accumulating a second record.
- **`validation_run`** — one row per execution with counts and a JSON breakdown. Data
  docs answer "what failed in this run"; this table answers the question a human asks
  next, which is "is this getting worse".

## Testing

19 tests against real PostgreSQL. The expensive ones stand up a full Great
Expectations context; most exercise the quarantine logic directly, which is fast.

CI checks out the pipeline project as a sibling rather than vendoring a copy of its
schema, so the tests break if the two drift apart — which is the point of having them.
It also imports the DAG, because a DAG that fails to import is a DAG that never runs
and Airflow only tells you at parse time.

## Layout

```
src/tradedq/
  rules.py       every rule, declared once, with both representations
  validate.py    runs the suites through Great Expectations
  quarantine.py  generates the quarantine SQL from the rules and applies it
  runs.py        validation_run history
  schema.py      rejected_trades and validation_run
  cli.py         rules / validate / history
dags/
  trade_data_quality.py   thin Airflow wrapper over the above
tests/           19 tests against real PostgreSQL
```

## Known limitations

- The DAG generates the feed it validates. A real deployment reads a file dropped by
  an upstream system; generating it keeps the stack runnable by someone with no access
  to a trade feed.
- `alert` writes a log line rather than paging anyone. Wiring Slack or PagerDuty into
  a portfolio project adds a credential and a dependency without demonstrating more.
- The connection string comes from an environment variable. A production deployment
  would use an Airflow Connection backed by a secrets manager, so the credential is
  not visible in `docker inspect`.
- Validation runs over the whole table every time. Fine at fifty thousand rows; at
  scale it would validate only the partition the load touched.
- Airflow's metadata shares a PostgreSQL instance with the data it orchestrates,
  which is convenient for a demo and wrong for production: a heavy analytical query
  should not be able to slow down the scheduler.
