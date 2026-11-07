"""Compiler-side behavior that needs no JVM: planning, watch selection, scaffolding, coverage."""
import os

from cascade_test import plan as P
from cascade_test import scaffold, watch
from cascade_test.project import Project
from cascade_test.spec import discover

EXAMPLE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples", "orders")


def test_plan_swaps_connectors_and_keeps_sink_semantics(tmp_path):
    project = Project(EXAMPLE)
    case = next(c for c in discover(EXAMPLE) if c.name == "one_day_of_orders_end_to_end")
    prep = P.prepare(project, case, str(tmp_path))
    assert prep.error is None
    sql = P.script(prep)
    assert "'connector' = 'kafka'" not in sql and "'connector' = 'iceberg'" not in sql
    assert "'connector' = 'cascade-fixture'" in sql
    assert "WATERMARK FOR `order_ts` AS SOURCE_WATERMARK()" in sql
    assert "'mode' = 'upsert'" in sql and "'mode' = 'append'" in sql  # captures keep the sinks' changelog modes
    roles = {o.label: o.role for o in prep.requests[0].outputs}
    assert roles["ltv_to_kafka"] == "sink" and roles["clean_orders.quarantined"] == "sink"
    assert roles["clean_orders"] == "expectations"


def test_plan_errors_are_friendly(tmp_path):
    project = Project(EXAMPLE)
    case = discover(EXAMPLE)[0]
    case.target = "customer_lvt"
    assert "did you mean 'customer_ltv'" in P.prepare(project, case, str(tmp_path)).error
    case.target, case.given = "customer_ltv", {"order": []}
    assert "did you mean 'orders'" in P.prepare(project, case, str(tmp_path)).error


def test_watch_reruns_only_affected_tests():
    project = Project(EXAMPLE)
    cases = discover(EXAMPLE)
    sql = os.path.abspath(os.path.join(EXAMPLE, "models", "revenue_per_minute.sql"))
    names = {c.name for c in watch.affected(project, cases, {sql})}
    assert "revenue_snapshot" in names and "one_day_of_orders_end_to_end" in names
    assert "ltv_sums_amounts_per_customer" not in names
    tf = cases[0].file
    assert {c.file for c in watch.affected(project, cases, {tf})} == {tf}
    assert len(watch.affected(project, cases, {os.path.join(EXAMPLE, "pipeline.yaml")})) == len(cases)


def test_scaffold_builds_a_test_for_every_upstream_source():
    t = scaffold.build(Project(EXAMPLE), "orders_enriched")
    assert set(t["given"]) == {"orders", "customers"}
    assert t["given"]["orders"][0]["order_ts"] == "+0m"


def test_nondeterministic_columns():
    project = Project(EXAMPLE)
    assert project.nondeterministic_columns("clean_orders.quarantined") == ["_quarantined_at"]
    assert project.nondeterministic_columns("customer_ltv") == []
