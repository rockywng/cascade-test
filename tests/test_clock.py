"""The frozen clock: wall-clock functions replaced by literals."""
import os
import shutil
import textwrap
from datetime import datetime, timezone

import pytest

from cascade_test import clock as C, runner
from cascade_test.engine import EngineError, dsl_jars, find_flink_home, shared_engine
from cascade_test.project import Project
from cascade_test.spec import discover

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.join(os.path.dirname(HERE), "examples", "orders")
T0 = datetime(2026, 11, 6, 12, 30, 15, 250000, tzinfo=timezone.utc)


def test_instants():
    assert C.instant("2026-11-06 12:30:15.25") == T0
    assert C.instant("2026-11-06T14:30:15.250+02:00") == T0
    assert C.instant("base_time", "2026-11-06 12:30:15.25") == T0
    with pytest.raises(C.ClockError, match="cannot read clock"):
        C.instant("yesterday")


def test_freeze_replaces_each_wall_clock_function():
    ms = int(T0.timestamp() * 1000)
    sql, n = C.freeze("SELECT CURRENT_TIMESTAMP, now(), LOCALTIMESTAMP, CURRENT_DATE, CURRENT_TIME, "
                      "UNIX_TIMESTAMP(), CURRENT_ROW_TIMESTAMP()", T0)
    assert n == 7
    assert sql == (f"SELECT TO_TIMESTAMP_LTZ({ms}, 3), TO_TIMESTAMP_LTZ({ms}, 3), TIMESTAMP '2026-11-06 12:30:15.250', "
                   f"DATE '2026-11-06', TIME '12:30:15', CAST({ms // 1000} AS BIGINT), TO_TIMESTAMP_LTZ({ms}, 3)")


def test_freeze_leaves_strings_identifiers_comments_and_other_names_alone():
    text = ("SELECT 'CURRENT_TIMESTAMP', `now`, t.now, now_col, CURRENT_DATE_X, PROCTIME(), \"CURRENT_DATE\" "
            "-- NOW() here\n/* CURRENT_TIME */ FROM t WHERE x = 'it''s NOW()' AND now > 1")
    assert C.freeze(text, T0) == (text, 0)


def _available() -> bool:
    try:
        dsl_jars()
    except EngineError:
        return False
    return find_flink_home() is not None


@pytest.mark.skipif(not _available(), reason="needs FLINK_HOME and cascade-dsl's runtime/build.sh")
def test_a_frozen_clock_makes_current_timestamp_deterministic(tmp_path):
    d = tmp_path / "orders"
    shutil.copytree(EXAMPLE, d, ignore=shutil.ignore_patterns("tests", "recordings"))
    pipeline = (d / "pipeline.yaml").read_text()
    pipeline = pipeline.replace("UPPER(currency) AS currency, order_ts\n", "UPPER(currency) AS currency, order_ts,\n"
                                "             CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS seen_at\n", 1)
    pipeline = pipeline.replace("      order_ts: TIMESTAMP(3)\n    expectations:", "      order_ts: TIMESTAMP(3)\n"
                                "      seen_at: TIMESTAMP(3)\n    expectations:", 1)
    (d / "pipeline.yaml").write_text(pipeline)
    (d / "tests").mkdir()
    (d / "tests" / "t.yaml").write_text(textwrap.dedent("""
        tests:
          - name: frozen
            target: clean_orders
            clock: "2026-11-06 12:30:15"
            given:
              orders: [{order_id: o1, customer_id: c1, amount: 10, currency: usd, order_ts: "2026-11-06 09:00:00"}]
            expect:
              rows:
                - {order_id: o1, customer_id: c1, amount: 10, currency: USD, order_ts: "2026-11-06 09:00:00", seen_at: "2026-11-06 12:30:15"}
          - name: not_frozen
            target: clean_orders
            given:
              orders: [{order_id: o1, customer_id: c1, amount: 10, currency: usd, order_ts: "2026-11-06 09:00:00"}]
            expect:
              rows:
                - {order_id: o1, customer_id: c1, amount: 10, currency: USD, order_ts: "2026-11-06 09:00:00", seen_at: "2026-11-06 12:30:15"}
          - name: bad_clock
            target: clean_orders
            clock: "someday"
            given:
              orders: [{order_id: o1, customer_id: c1, amount: 10, currency: usd, order_ts: "2026-11-06 09:00:00"}]
            expect: {count: 1}
    """))
    project = Project(str(d))
    results = {r.case.name: r for r in runner.run(project, discover(project.root), shared_engine(), runner.Options())}
    assert results["frozen"].passed, results["frozen"].message
    assert not results["not_frozen"].passed  # the real clock is not 2026-11-06
    assert "bad clock" in results["bad_clock"].message
