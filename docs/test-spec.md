# Test file reference

Test files are `tests/**/*.yaml` in a Cascade project (next to `pipeline.yaml` or `pipeline.py`).
Every file that `cascade test` (cascade-dsl) accepts works unchanged; everything below is
optional.

```yaml
defaults:            # file-wide; any test can override
fixtures:            # named inputs that tests pull in with `use:`
tests:               # the tests
```

## A test

```yaml
tests:
  - name: ltv_sums_amounts_per_customer     # unique within the file
    description: optional text
    target: customer_ltv                    # see "Targets"
    use: [acme]                             # named fixtures, merged before `given`
    given: {orders: [...], customers: [...]}
    expect: {...}                           # see "Expectations"

    # optional
    ordered: false            # compare rows (and changelogs) in order
    key: [customer_id]        # match rows by key in diffs (default: sink PK / model upsert key)
    tolerance: 0.01           # absolute numeric tolerance; or per column: {revenue: 0.01}
    ignore_columns: [loaded_at]
    watermarks: per_row       # per_row | manual | end_of_input
    partitions: {orders: 4}   # declared source partitions (empty ones hold back the watermark)
    base_time: "2026-01-01 10:00:00"
    defaults: {orders: {currency: usd}}
    parallelism: 2            # default 1
    config: {table.exec.mini-batch.enabled: "false"}   # Flink settings for this test
    timeout: 60               # seconds
    tags: [slow]              # select with -t slow, exclude with -t '!slow'
    skip: "reason"            # or true
    only: true                # run only the tests marked `only`
    parameters: ...           # see "Parameterized tests"
```

## Targets

| target | what runs | compared as |
|---|---|---|
| a model | the model's view | final state (changelog applied) |
| `<model>.quarantined` | rows rejected by `on_violation: quarantine` checks, with `_violated_expectation` | final state |
| a sink | the sink's real `INSERT` (upsert materialization, `ON CONFLICT`) into a capture with the sink's changelog mode | what the external table holds: by primary key for upsert sinks |
| a source | the fixture as Flink reads it | final state |
| `pipeline` | the whole statement set as one job; every sink and quarantine captured | per sink, under `expect.sinks` |

## Inputs (`given`)

A source's input is a list of rows and markers, replayed in order:

```yaml
given:
  orders:
    - {order_id: o1, amount: 10.00, order_ts: "2026-01-01 10:00:00"}
    - {order_id: o2, amount: 5.00, order_ts: "+5m"}       # relative to base_time
    - {_partition: 1, order_id: o3, order_ts: "+6m"}       # Kafka partition (default 0)
    - {_watermark: "+10m"}                                # emit a watermark now
    - {_idle: 1}                                          # partition 1 goes idle
  customers:                                              # changelog sources
    - {customer_id: c1, segment: smb}                     # _op: insert (default)
    - {_op: update, customer_id: c1, segment: enterprise} # merged onto the previous row
    - {_op: upsert, customer_id: c2, segment: mid}        # insert or update
    - {_op: delete, customer_id: c1}
```

Unlisted sources are empty. Unknown columns are errors, with a suggestion.

**Defaults and templates.** `defaults.rows.<source>` (file-wide) and `defaults.<source>` (per
test) fill in columns a row does not mention. In defaults and rows, `{n}` is the row's number
(1-based) and `{i}` its index, so `{order_id: "o{n}", order_ts: "+{n}m"}` gives every row a unique
id and a time one minute after the previous one.

**Times.** Timestamps are UTC unless they carry an offset. `+5m`, `-30s`, `+1h30m`, `+250ms` are
relative to `base_time` (default `2026-01-01 00:00:00`), in inputs and in expected rows.

**Files.** `orders: {file: fixtures/orders.csv}`: CSV with a header (empty cells are null),
`.jsonl`, `.json` or `.yaml` (a list of rows); `.gz` works. Paths are relative to the test file.
A mapping can combine `file`, `generate` and `rows` (in that order), plus `partitions` and
`defaults`.

**Generated data.**

```yaml
orders:
  generate:
    count: 1000
    seed: 7                       # same seed, same rows
    step: 1s                      # event-time spacing between rows
    jitter: 2s                    # +- random skew (out-of-order data)
    columns:
      customer_id: {one_of: [c1, c2, c3]}
      amount: {between: [1, 100]}  # typed by the column (DECIMAL keeps its scale)
      note: {null_rate: 0.1}
      order_id: "o{n}"
  partitions: 4                    # rows are spread round-robin
```

Columns without a spec get typed random values; primary key columns are unique.

## Watermarks

| mode | behavior |
|---|---|
| `per_row` (default) | after each row, each partition's watermark is `max event time - max_out_of_orderness`; the source watermark is the minimum over partitions that are not idle and does not advance until every active partition has data |
| `manual` | only `_watermark` rows advance event time |
| `end_of_input` | no watermark until the end |

At the end of input, event time jumps to +∞ and every remaining window fires. A row whose event time
is at or behind the source watermark when it arrives is *late*; `expect.late` asserts which rows,
failure output lists them, and `cascade-test timeline` shows the whole sequence.

Sources are replayed by one reader each, so the order within a source is exact. The interleaving
*between* sources is not controlled, so lateness that depends on it (one source racing another
into an interval join) can vary.

## Expectations

```yaml
expect:
  rows: [...]            # the final output is exactly these rows
  contains: [...]        # at least these rows
  excludes: [...]        # none of these rows
  count: 3               # or {min: 1, max: 10}
  empty: true            # same as rows: []
  changelog:             # the changes themselves (_op: insert|update_before|update_after|delete or +I/-U/+U/-D)
    - {_op: insert, customer_id: c1, lifetime_value: 10}
  snapshot: true         # compare with tests/__snapshots__/<file>.snap.yaml
  invariants:            # SQL predicates every output row satisfies (checked by Flink)
    - lifetime_value > 0
  expectations:          # data-quality checks: violating rows seen (before drop/quarantine/fail)
    positive_amount: 1   # or clean_orders.positive_amount: 1
  late: {orders: [3]}    # these input rows (1-based) were late; or a count: {orders: 0}
  error: has_customer    # the job must fail with an error mentioning this text
  sinks:                 # target: pipeline only
    ltv_to_kafka: {rows: [...]}
    clean_orders.quarantined: {count: 1}
```

Rows list only the columns they care about; a column a row does not mention matches anything.
Values are compared by the output column's Flink type: `10`, `10.0` and `"10.00"` are the same
DECIMAL; timestamps are parsed (relative times allowed).

When rows have a key (a sink's primary key, a model's upsert key, or `key:`), a wrong value is
reported as one changed row with the differing columns:

```
FAIL  wrong_value  (customer_ltv, 210 ms)
      rows differ: 1 changed, 1 missing (0 matched)
        rows matched by key (customer_id); compared columns: customer_id, lifetime_value, order_count
          ~ customer_id=c1
              lifetime_value: expected 16, got 15
          expected, not produced:
          - {customer_id: c2, lifetime_value: 1}
```

## Function calls

```yaml
- name: masking
  calls:
    - {call: "MASK_EMAIL('alice@example.com')", returns: "a***@example.com"}
    - {call: "CAST('12.345' AS DECIMAL(12, 2))", returns: 12.35, type: "DECIMAL(12, 2) NOT NULL"}
    - {call: "CAST('x' AS INT)", error: NumberFormatException}
```

Each call runs as a SQL expression in Flink with the pipeline's functions registered, so Java and
Python UDFs are checked with real type conversion and NULL handling.

## Parameterized tests

```yaml
- name: non_positive_amounts_are_dropped
  target: clean_orders
  parameters:                   # a mapping (named cases) or a list
    zero: {amount: 0}
    negative: {amount: -3.00}
  given: {orders: [{order_id: bad, amount: "${amount}"}]}
  expect: {rows: []}
```

Each entry becomes its own test, `name[case]`. `"${x}"` alone keeps the parameter's type;
inside a longer string it is substituted as text.

## Snapshots

`expect: {snapshot: true}` (or per sink in a pipeline test) records the output in
`tests/__snapshots__/<file>.snap.yaml` on the first run; commit it. Later runs diff against it.
`--update-snapshots` (`-u`) re-records after an intended change; `--ci` fails when a snapshot is
missing instead of recording it. Columns computed from processing time or randomness
(`CURRENT_TIMESTAMP`, `PROCTIME()`, `RAND()`, `UUID()`, non-deterministic UDFs, `_quarantined_at`)
are left out.
