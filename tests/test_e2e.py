"""End-to-end tests against a real Flink (skipped without FLINK_HOME and cascade-dsl's JVM jars)."""
import os
import shutil
import textwrap

import pytest

from cascade_test import cli, runner
from cascade_test.engine import EngineError, dsl_jars, find_flink_home, shared_engine
from cascade_test.project import Project
from cascade_test.spec import discover

HERE = os.path.dirname(os.path.abspath(__file__))
EXAMPLE = os.path.join(os.path.dirname(HERE), "examples", "orders")
RECORDING = os.path.join(EXAMPLE, "recordings", "2026-11-06")


def _available() -> bool:
    try:
        dsl_jars()
    except EngineError:
        return False
    return find_flink_home() is not None


pytestmark = pytest.mark.skipif(not _available(), reason="needs FLINK_HOME and cascade-dsl's runtime/build.sh")


@pytest.fixture(scope="module")
def engine():
    return shared_engine()


@pytest.fixture()
def project_copy(tmp_path):
    """A copy of the example project with its tests replaced by `tests` (YAML text)."""
    def make(tests: str, edit=None) -> str:
        d = tmp_path / "orders"
        shutil.copytree(EXAMPLE, d, ignore=shutil.ignore_patterns("tests", "recordings"))
        (d / "tests").mkdir()
        (d / "tests" / "t.yaml").write_text(textwrap.dedent(tests))
        if edit:
            p = d / "pipeline.yaml"
            p.write_text(edit(p.read_text()))
        return str(d)
    return make


def run(path, engine, **opts):
    project = Project(path)
    return runner.run(project, discover(project.root), engine, runner.Options(**opts))


def by_name(results):
    return {r.case.name: r for r in results}


def test_example_suite_passes(engine):
    results = run(EXAMPLE, engine, ci=True, coverage=True)
    bad = [f"{r.case.name}: {r.message}" for r in results if not r.passed]
    assert not bad, "\n".join(bad)
    assert len(results) >= 18


def test_cli_run_writes_reports(tmp_path):
    junit, md, js = tmp_path / "j.xml", tmp_path / "r.md", tmp_path / "r.json"
    code = cli.main(["run", EXAMPLE, "-k", "ltv", "--junit", str(junit), "--markdown", str(md), "--json", str(js),
                     "--no-color"])
    assert code == 0
    assert "<testsuite" in junit.read_text() and "Pipeline tests" in md.read_text() and '"status": "pass"' in js.read_text()


FAILING = """
defaults:
  base_time: "2026-01-01 10:00:00"
  rows: {orders: {order_id: "o{n}", customer_id: c1, amount: 10.00, currency: usd, order_ts: "+{n}m"}}
tests:
  - name: wrong_value
    target: customer_ltv
    given: {orders: [{}, {amount: 5}]}
    expect: {rows: [{customer_id: c1, lifetime_value: 16.00}]}
  - name: wrong_count_and_invariant
    target: orders_per_minute
    given: {orders: [{}, {}]}
    expect: {count: 1, invariants: ["orders > 1"]}
  - name: wrong_expectation_and_late
    target: orders_per_minute
    given: {orders: [{amount: 0}, {order_ts: "+10m"}, {order_ts: "+1m"}]}
    expect: {expectations: {positive_amount: 2}, late: {orders: 0}}
  - name: unexpected_job_failure
    target: clean_orders
    given: {orders: [{customer_id: null}]}
    expect: {count: 0}
  - name: typo
    target: customer_ltv
    given: {orders: [{}]}
    expect: {rows: [{lifetime_valu: 1}]}
  - name: wrong_call
    calls: [{call: "UPPER('a')", returns: b}]
"""


def test_failures_are_reported_precisely(project_copy, engine):
    r = by_name(run(project_copy(FAILING), engine))
    assert all(x.status == "fail" for x in r.values())
    assert "lifetime_value: expected 16, got 15" in r["wrong_value"].message
    assert "expected 1 row(s), got 2" in r["wrong_count_and_invariant"].message
    assert "invariant violated" in r["wrong_count_and_invariant"].message
    m = r["wrong_expectation_and_late"].message
    assert "expected 2 violating row(s), got 1" in m and "expected 0 late row(s) in orders, got 1" in m
    assert "ExpectationViolation" in r["unexpected_job_failure"].message
    assert "did you mean 'lifetime_value'" in r["typo"].message
    assert "expected b, got A" in r["wrong_call"].message


def test_append_sink_fed_an_updating_stream_fails_like_production(project_copy, engine):
    path = project_copy("""
        tests:
          - name: ltv_sink
            target: ltv_to_kafka
            given: {orders: [{order_id: o1, customer_id: c1, amount: 1, currency: usd, order_ts: "+0s"}]}
            expect: {count: 1}
    """, edit=lambda s: s.replace("    mode: upsert\n    primary_key: [customer_id]\n", "    mode: append\n"))
    try:
        results = run(path, engine)
    except Exception as e:  # the compiler may already reject it statically, which is just as good
        assert "append" in str(e).lower() or "updat" in str(e).lower()
        return
    (r,) = results
    assert r.status == "fail"
    assert "update changes" in r.message or "updating" in r.message


def test_snapshots_record_detect_and_update(project_copy, engine):
    path = project_copy("""
        tests:
          - name: snap
            target: customer_ltv
            given: {orders: [{order_id: o1, customer_id: c1, amount: 1, currency: usd, order_ts: "+0s"}]}
            expect: {snapshot: true}
    """)
    snap = os.path.join(path, "tests", "__snapshots__", "t.snap.yaml")
    (r,) = run(path, engine, ci=True)
    assert r.status == "fail" and "no snapshot" in r.message and not os.path.exists(snap)
    (r,) = run(path, engine)
    assert r.passed and os.path.exists(snap)
    text = open(snap).read()
    open(snap, "w").write(text.replace("'1.00'", "'2.00'"))
    (r,) = run(path, engine)
    assert r.status == "fail" and "differs from its snapshot" in r.message
    (r,) = run(path, engine, update_snapshots=True)
    assert r.passed and "'1.00'" in open(snap).read()


def test_replay_is_deterministic_and_finds_changes(tmp_path, engine):
    from cascade_test.replay import load_recording, replay
    rec = load_recording(RECORDING)
    base = Project(EXAMPLE)
    same = replay(engine, base, rec, baseline=Project(EXAMPLE), baseline_label="self")
    assert not same.changed, same.text()
    assert {s.sink for s in same.sinks} == {"ltv_to_kafka", "revenue_to_lake", "clean_orders.quarantined"}
    assert same.excluded == {"clean_orders.quarantined": ["_quarantined_at"]}

    v2 = tmp_path / "v2"
    shutil.copytree(EXAMPLE, v2, ignore=shutil.ignore_patterns("recordings"))
    p = v2 / "pipeline.yaml"
    p.write_text(p.read_text().replace("('USD', 'EUR', 'GBP')", "('USD', 'EUR', 'GBP', 'JPY')"))
    diff = replay(engine, Project(str(v2)), rec, baseline=base, baseline_label="main")
    assert diff.changed
    sinks = {s.sink: s for s in diff.sinks}
    assert sinks["clean_orders.quarantined"].candidate_rows == 0
    assert sinks["ltv_to_kafka"].diff.changed and not sinks["ltv_to_kafka"].diff.missing
    assert "Replay diff" in diff.markdown()


def test_golden_outputs(tmp_path, engine):
    from cascade_test.replay import load_recording, replay
    rec = load_recording(RECORDING)
    golden = str(tmp_path / "golden")
    first = replay(engine, Project(EXAMPLE), rec, golden=golden, last="30m")
    assert os.path.exists(os.path.join(golden, "ltv_to_kafka.jsonl"))
    again = replay(engine, Project(EXAMPLE), rec, golden=golden, last="30m")
    assert not again.changed, again.text()
    assert first.replayed == again.replayed


def test_python_api(engine):
    from cascade_test import project
    orders = project(os.path.join(EXAMPLE, "tests", "anything.py"), engine=engine)
    r = orders.run("customer_ltv", given={"orders": [
        {"order_id": "o1", "customer_id": "c1", "amount": 10, "currency": "usd", "order_ts": "+0s"}]})
    r.assert_rows([{"customer_id": "c1", "lifetime_value": 10}])
    with pytest.raises(AssertionError, match="lifetime_value: expected 11, got 10"):
        r.assert_rows([{"customer_id": "c1", "lifetime_value": 11}])
    assert orders.call("1 + 1") == 2
