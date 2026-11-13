"""cascade-test trace: input rows followed through every model and sink."""
import os

import pytest

from cascade_test import trace as T
from cascade_test.engine import EngineError, dsl_jars, find_flink_home, shared_engine
from cascade_test.project import Project

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.join(os.path.dirname(HERE), "examples", "orders")
RECORDING = os.path.join(EXAMPLE, "recordings", "2026-11-06")


def _available() -> bool:
    try:
        dsl_jars()
    except EngineError:
        return False
    return find_flink_home() is not None


needs_flink = pytest.mark.skipif(not _available(), reason="needs FLINK_HOME and cascade-dsl's runtime/build.sh")


def test_stages_run_inputs_first_then_sinks():
    order = T.stage_order(Project(EXAMPLE))
    assert order.index("clean_orders") < order.index("orders_enriched") < order.index("revenue_per_minute")
    assert order[-2:] == ["revenue_to_lake", "ltv_to_kafka"]


def test_sample_is_seeded_and_small():
    project = Project(EXAMPLE)
    a, _ = T.sample_given(project, RECORDING, 3, 7, [])
    b, _ = T.sample_given(project, RECORDING, 3, 7, [])
    c, _ = T.sample_given(project, RECORDING, 3, 8, [])
    assert a == b and a != c
    assert {len(rows) for rows in a.values()} == {3}


def test_given_file_must_map_sources_to_rows(tmp_path):
    f = tmp_path / "g.yaml"
    f.write_text("- not a mapping\n")
    with pytest.raises(T.TraceError, match="mapping of source"):
        T.load_given(str(f))


def test_render_points_at_the_stage_that_loses_the_rows():
    stages = [{"stage": "a", "kind": "model", "rows": [{"x": 1}], "columns": ["x"], "rejected": {}, "error": None},
              {"stage": "b", "kind": "model", "rows": [], "columns": ["x"], "rejected": {"b.positive": 1}, "error": None}]
    text = T.render(stages, {"src": [{"x": 1}]})
    assert "model b: 0 row(s)  <- nothing comes out (rejected by b.positive: 1)" in text
    assert "model a: 1 row(s)" in text


@needs_flink
def test_trace_a_sample_end_to_end():
    project = Project(EXAMPLE)
    given, parts = T.sample_given(project, RECORDING, 3, 1, [])
    stages = {s["stage"]: s for s in T.trace(shared_engine(), project, given, parts)}
    assert not [s for s in stages.values() if s["error"]]
    assert len(stages["clean_orders"]["rows"]) == 3
    assert len(stages["orders_enriched"]["rows"]) == 3
    assert stages["revenue_to_lake"]["rows"], "the sink saw the sampled orders"


@needs_flink
def test_trace_only_the_named_stage():
    project = Project(EXAMPLE)
    given, parts = T.sample_given(project, RECORDING, 2, 1, [])
    (only,) = T.trace(shared_engine(), project, given, parts, stages=["clean_orders"])
    assert only["stage"] == "clean_orders" and len(only["rows"]) == 2
