A real Flink 2.3 savepoint (canonical format, state inline in `_metadata`, 24 KiB) of two keyed
aggregations over one datagen source, written to blackhole sinks:

    INSERT INTO s1 SELECT k, SUM(v)    FROM src GROUP BY k
    INSERT INTO s2 SELECT k, COUNT(*)  FROM src GROUP BY k

Eight operators; two hold state: `e03d9229619566fd5d62dea3a8d25d1e` and
`570f707193e0fe32f4d86d067aba243b` (about 12 KiB each). Taken with `JobClient.stopWithSavepoint`
after six seconds. Used by tests/test_state.py.
