# Design

cascade-test is the test framework of the Cascade platform (design doc §7: "Test framework, a
headline feature"). It covers the test pyramid's unit, integration and replay layers and provides
the diff engine the shadow/canary layer needs.

| layer | what runs | here |
|---|---|---|
| Unit | one model or UDF on in-memory fixtures with event-time scripts | `target: <model>`, `calls:`, `_watermark`, `_partition`, `_idle` |
| Integration | the whole pipeline, sources and sinks replaced | `target: pipeline`; captures keep each sink's changelog mode |
| Replay | recorded production data against a candidate version | `cascade-test replay`, `cascade-test ci` |
| Shadow / canary | candidate next to prod on live traffic, sinks redirected | deployment is the control plane's job; `cascade-test diff` compares the two sides' sink dumps |

## Components

```
tests/*.yaml ─► spec ─► plan ──────────────► engine (JVM, one per session) ─► runner ─► report
                 │       │ fixtures.py         │ shared MiniCluster              │ diff.py
 pipeline.yaml ─► cascade-dsl compiler         │ several tests at once           │ snapshots, coverage
                   (statements per kind)       │ cascade-capture sink            │ console, JUnit, Markdown
```

**Compiler integration.** Tests run the statements cascade-dsl generates for production
(`Lowered.statements`, typed by kind), with three substitutions: sources read fixtures, sinks
under test write to a capture with the same changelog mode, other sinks are dropped. Nothing is
pattern-matched in SQL text, so tests exercise the same views, expectation functions, casts and
`INSERT`s that deploy.

**Engine.** Starting Flink dominates the cost of a pipeline test. Flink's local executor starts a
MiniCluster per job; the engine instead starts one cluster per session and installs a context
environment so every `TableEnvironment` submits to it. It runs the tests of a request
concurrently (each in its own `TableEnvironment`, since Calcite keeps planner state in
thread-locals) and streams JSON lines over stdin/stdout. All outputs of a test (target, sinks,
invariant checks, expectation probes) run as one job, a statement set. The jar is compiled on
first use against `$FLINK_HOME` and cached per Flink build, so there is no build step and no
version skew.

**Fixtures and time.** Watermarks are computed in Python, deterministically, and handed to the
fixture source as explicit watermark events; tables declare `WATERMARK ... AS SOURCE_WATERMARK()`
so those are the only ones. Partitions follow Flink's per-split semantics (minimum over active
splits, no progress until every active split has data, idle splits excluded). Lateness is
computed at the same point, which is what makes "row 3 was late" an assertion rather than a guess.

**Captures.** `cascade-capture` records every change in memory (the MiniCluster runs in the same
JVM) with the changelog mode of the sink it replaces: `append` rejects updating input at planning
time, `upsert` gets Flink's upsert materialization, `all` (for models) records retractions.

**Diff engine.** One implementation behind test assertions, snapshots, replay and `diff`:
changelog normalization (retract or upsert by key) to final state, key-aware matching (a wrong
value is one changed row with the columns that differ), order-insensitive within a key, numeric
tolerance, and partial rows (unlisted columns match anything). Renders as text, Markdown and JSON.

## Replay

- **Capture.** A recording keeps partition, offset, record timestamp, key and headers per record,
  in capture order (`cascade-test record` imports `kcat -C -J` dumps; Debezium envelopes become
  changelog records). Lookup tables are recorded as cassettes.
- **Determinism.** Partitions replay in recorded order (or by record timestamp), with per-partition
  watermarks as above; lookups are served from cassettes; columns computed from processing time or
  randomness are excluded from the diff and listed; recordings that start mid-stream are handled
  leniently (an update of an unseen key becomes an insert, a delete of one is skipped).
- **Sink redirection.** Every sink is captured; a replay never writes anywhere.
- **Diff.** Candidate vs a baseline version (a path or `git:<ref>`, extracted with `git archive`)
  run on the same input in the same engine, or vs golden outputs. Upsert sinks compare by primary
  key; `--key sink=cols` gives append sinks a key; `--tolerance`, `--ignore`.
- **CI.** `cascade-test ci` adds cascade-dsl's state-compatibility diff (would the change reset
  state, and for which operators?) and writes one Markdown report for the PR comment.

Not covered yet: starting a replay from a production savepoint (needs savepoint access and the
State Processor API to sanitize state) and recording directly from Kafka (use `kcat -C -J`).

## Extras beyond the design doc

- `cascade-test watch`: re-runs affected tests on save on a warm engine (well under a second).
- Snapshots, parameterized tests, named fixtures, defaults/templates, relative times, CSV/JSONL
  fixtures, seeded data generation, invariants.
- Pipeline coverage: models asserted, sinks tested, data-quality checks actually seen to fire.
- `cascade-test timeline`: the event-time story of a test, row by row.
- `cascade-test new --record`: scaffold a test from the pipeline's schemas and fill in its output.
- pytest plugin and a Python API, JUnit XML, GitHub annotations, step summaries, a GitHub Action.
- Failure messages: did-you-mean for targets/columns/sources/keys, hints for common Flink errors,
  root causes instead of exception wrappers, the actual output and late rows next to a failure.
