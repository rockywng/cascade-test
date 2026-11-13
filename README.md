# cascade-test

The test framework for [Cascade](../cascade-control-plane) pipelines. Write tests as a few lines of
YAML (or Python), run them on a real Flink (a few hundred milliseconds each once the engine is warm;
`watch` re-runs the affected ones in under a second after a save), and get diffs that say which key
and which column changed. The same machinery replays recorded production data through a new
version and posts the differences on the pull request.

```yaml
# examples/orders/tests/models_test.yaml
defaults:
  base_time: "2026-01-01 10:00:00"
  rows:
    orders: {order_id: "o{n}", customer_id: c1, amount: 10.00, currency: usd, order_ts: "+{n}m"}

tests:
  - name: ltv_sums_amounts_per_customer
    target: customer_ltv
    given:
      orders:
        - {amount: 10.00}
        - {amount: 5.50}
        - {customer_id: c2, amount: 7.00}
    expect:
      rows:
        - {customer_id: c1, lifetime_value: 15.50, order_count: 2}
        - {customer_id: c2, lifetime_value: 7.00, order_count: 1}
```

```
$ cascade-test examples/orders
PASS  ltv_sums_amounts_per_customer  (customer_ltv, 214 ms)
PASS  late_order_misses_its_closed_window  (orders_per_minute, 198 ms)
...
18 passed in 5.2s
```

It builds on [cascade-dsl](https://github.com/rockywng/cascade-dsl): tests run the statements the
compiler generates for production, with sources replaced by fixtures and sinks by captures that keep
each sink's changelog mode. Test files written for `cascade test` work unchanged.

## Setup

Same layout as the other Cascade repositories: check out cascade-dsl next to this one as `../pipedsl`.

```bash
git clone https://github.com/rockywng/cascade-dsl ../pipedsl
```

```bash
python3 -m venv .venv && .venv/bin/pip install -e ../pipedsl -e '.[test]'
```

```bash
export FLINK_HOME=~/flink-2.3.0 && ../pipedsl/runtime/build.sh
```

```bash
.venv/bin/cascade-test doctor
```

Needs Python 3.10+, a JDK 11+ and a Flink 2.x distribution. The engine jar is compiled against
`$FLINK_HOME` on first use and cached, so there is no separate build step.

## What a test can say

| | |
|---|---|
| **Targets** | a model, `<model>.quarantined`, a sink (its real `INSERT`, upsert key and changelog mode), or `pipeline` (every sink at once) |
| **Inputs** | rows with defaults and `{n}` templates, relative times (`+5m`), CSV/JSONL files, seeded generated data, changelog ops (`_op: update`), Kafka partitions (`_partition`), explicit and idle-partition watermarks |
| **Expectations** | `rows` (partial: only the columns you name), `contains`, `excludes`, `count`, `changelog`, `snapshot`, `invariants` (SQL predicates), data-quality `expectations` (violations seen), `late` rows, `error` |
| **Structure** | `parameters` (one test per case), named `fixtures` with `use:`, file-wide `defaults`, `tags`, `skip`, `only`, `calls` for UDFs and SQL expressions |

Full reference: [docs/test-spec.md](docs/test-spec.md). The example project
[examples/orders](examples/orders) uses every feature.

### Event time you can reason about

Watermarks are computed deterministically from the source's declared strategy, per Kafka
partition, the way Flink does: the minimum over partitions that are not idle. A slow partition
holds back every window; an idle one stops counting. Rows at or behind the watermark are *late*,
and the framework knows which ones:

```yaml
  - name: a_slow_partition_holds_back_the_watermark
    target: orders_per_minute
    partitions: {orders: 2}
    given:
      orders:
        - {_partition: 1, order_ts: "+5s"}
        - {_partition: 0, order_ts: "+3m"}
        - {_partition: 0, order_ts: "+30s"}   # on time: partition 1 is still at 10:00:05
    expect:
      rows: [{window_start: "+0m", orders: 2}, {window_start: "+3m", orders: 1}]
      late: {orders: 0}
```

```
$ cascade-test timeline examples/orders -k idle_partition
orders  event time order_ts, max_out_of_orderness 5000 ms, watermarks per_row, partitions [0, 1]
     #  part  op  event time               watermark after          row
     1     1  +I  2026-01-01 10:00:05      -                        order_id: o1, ...
     2     0  +I  2026-01-01 10:03:00      2026-01-01 10:00:00      order_id: o2, ...
           1  --                           2026-01-01 10:02:55      partition idle
     3     0  +I  2026-01-01 10:00:30      2026-01-01 10:02:55      order_id: o3, ...  LATE
```

### Failures that point at the problem

```
FAIL  wrong_value  (customer_ltv, 210 ms)
      rows differ: 1 changed, 1 missing (0 matched)
        rows matched by key (customer_id); compared columns: customer_id, lifetime_value, order_count
          ~ customer_id=c1
              lifetime_value: expected 16, got 15
          expected, not produced:
          - {customer_id: c2, lifetime_value: 1}
      actual output of customer_ltv (1 row(s)):
        customer_id  lifetime_value  order_count  last_order_ts
        c1           15.00           2            2026-01-01 10:02:00.000
      at tests/models_test.yaml:6
FAIL  unexpected_failure  (clean_orders, 180 ms)
      the job failed
        ExpectationViolation: expectation clean_orders.has_customer failed: customer_id IS NOT NULL
        hint: a data-quality expectation with `on_violation: fail` was violated; to test that, add `expect: {error: <expectation name>}`
ERROR typo_in_fixture  (customer_ltv, 0 ms)
      bad fixture: orders row 1: unknown column 'amout' (did you mean 'amount'?)
```

## Commands

| | |
|---|---|
| `cascade-test [run] [PATH...]` | run tests; `-k`, `-t tag`, `-u` (update snapshots), `--ci`, `--coverage`, `--min-coverage`, `--junit`, `--markdown`, `--json`, `--show-sql` |
| `cascade-test watch PATH` | re-run affected tests on save, on a warm engine |
| `cascade-test new PATH TARGET [--record]` | scaffold a test from the pipeline's schemas; `--record` fills in the actual output |
| `cascade-test timeline PATH -k NAME` | a test's event-time timeline: rows, watermarks, late rows |
| `cascade-test replay PATH --recording DIR --baseline git:main` | replay recorded data through two versions and diff every sink |
| `cascade-test record --source S --input dump.json -o DIR` | import a `kcat -C -J` dump (or JSON lines, Debezium) as a recording |
| `cascade-test diff A B --key k` | diff two captured outputs, e.g. a shadow job's sink vs production's |
| `cascade-test state inspect DIR` / `state subset DIR OUT --drop UID` | list a savepoint's operators, or copy it without some operators' state |
| `cascade-test ci PATH --baseline git:origin/main --recording DIR` | tests + coverage + state-compatibility + replay diff, one Markdown report |
| `cascade-test doctor` | check Java, Flink, cascade-dsl jars and the engine |

## pytest and Python

The pytest plugin installs with the package: `pytest examples/orders` runs the YAML tests as
pytest items next to Python tests, with `-k`, `-x`, IDE runners and JUnit. Python tests use the
same engine and assertions:

```python
from cascade_test import project

orders = project(__file__)          # the project this test file belongs to

def test_ltv_per_customer():
    r = orders.run("customer_ltv", given={"orders": [
        {"order_id": "o1", "customer_id": "c1", "amount": 10, "currency": "usd", "order_ts": "+0m"},
        {"order_id": "o2", "customer_id": "c1", "amount": 5, "currency": "usd", "order_ts": "+1m"},
    ]})
    r.assert_rows([{"customer_id": "c1", "lifetime_value": 15, "order_count": 2}])
    r.assert_expectation("positive_amount", violations=0)

def test_sinks():
    r = orders.pipeline_run(given={...})
    r.sink("ltv_to_kafka").assert_count(2)
    r.sink("revenue_to_lake").assert_all(lambda row: row["revenue"] > 0)
```

`Result` also has `rows` (Python values), `changelog`, `late_rows`, `expectations`,
`assert_contains/excludes/changelog/error/empty`, and `to_pandas()`.

## Replay and CI

Record a slice of production input, then replay it through the candidate and the deployed
version and compare every sink:

```bash
kcat -C -J -b $BROKER -t orders.v1 -o s@$(date -d '-6 hours' +%s000) -e > orders.json
```

```bash
cascade-test record --source orders --input orders.json -o recordings/2026-11-06
```

```bash
cascade-test replay . --recording recordings/2026-11-06 --baseline git:origin/main --markdown diff.md
```

```
replay: orders_analytics on recording '2026-11-06' (2425 records) vs `origin/main`
  ≠ clean_orders.quarantined: 51 removed (0 matched) (51 -> 0 rows)
  ≠ revenue_to_lake: 50 added (1258 matched) (1258 -> 1308 rows)
  ≠ ltv_to_kafka: 18 changed (2 matched) (20 -> 20 rows)
      ~ customer_id=c10
          lifetime_value: baseline 15797.38, candidate 16773.57
          order_count: baseline 112, candidate 120
  clean_orders.quarantined: left out of the diff (nondeterministic): _quarantined_at
```

Replays are deterministic: partitions keep their recorded interleaving and per-partition
watermarks, lookups come from recorded cassettes, sinks are captured (nothing is written
anywhere), and columns computed from processing time or randomness are left out and listed.
Replaying a version against itself reports no differences.

To exercise stateful logic against real state, start the replayed jobs from a production savepoint
(`--from-savepoint DIR`). Operators in the savepoint that the candidate no longer has are skipped
rather than failing the run. `--state-drop UID` (or `--state-keep UID`) first writes a copy of the
savepoint without (or with only) those operators' state, using Flink's State Processor API, so a
changed operator does not restore old state into a new query. `cascade-test state inspect DIR`
shows which operators hold how much state, and `cascade-test state subset DIR OUT --drop UID` writes
the copy on its own. Operators are named by their pinned UID or by the hex id `inspect` prints. The
unit is a whole operator: a SQL operator's keyed state can't be filtered by key without that
operator's code. Needs a Flink 2.x distribution that ships `opt/flink-state-processor-api`.

In GitHub Actions, the bundled action runs `cascade-test ci` and keeps one up-to-date comment on
the PR with test results, coverage, the state-compatibility verdict and the replay diff:

```yaml
- uses: rockywng/cascade-test@main
  with:
    project: pipelines/orders
    baseline: git:origin/${{ github.base_ref }}
    recording: pipelines/orders/recordings/latest
  env:
    CASCADE_DSL_TOKEN: ${{ secrets.CASCADE_DSL_TOKEN }}
```

## How it works

Tests run the statements cascade-dsl generates for production. Sources read fixtures with
`SOURCE_WATERMARK()`, so the watermarks computed by the framework are the only ones; sinks under
test write to `cascade-capture` with their own changelog mode, so an updating stream into an
append-only sink fails here as it would in production. A JVM engine keeps one MiniCluster for the
session and runs several tests at once, each as one job. Details: [docs/design.md](docs/design.md).

## Develop

```bash
.venv/bin/python -m pytest tests
```

```bash
.venv/bin/python -m pytest examples/orders
```

Tests that need Flink skip without `FLINK_HOME` and cascade-dsl's JVM jars.
