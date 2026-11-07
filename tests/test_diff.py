from decimal import Decimal

from cascade_test import diff as D

TYPES = {"id": "STRING", "amount": "DECIMAL(10, 2)", "n": "BIGINT", "ts": "TIMESTAMP(3)"}


def test_identical_rows_match_in_any_order():
    a = [{"id": "a", "n": 1}, {"id": "b", "n": 2}]
    d = D.compare(a, list(reversed(a)), TYPES)
    assert d.ok and d.matched == 2


def test_values_compare_by_column_type():
    d = D.compare([{"amount": 10, "n": "3", "ts": "2026-01-01T10:00:00Z"}],
                  [{"amount": "10.00", "n": 3, "ts": "2026-01-01 10:00:00.000"}], TYPES)
    assert d.ok


def test_relative_times_resolve_against_base_time():
    d = D.compare([{"ts": "+5m"}], [{"ts": "2026-01-01 10:05:00.000"}], TYPES, base_time="2026-01-01 10:00:00")
    assert d.ok


def test_key_aware_diff_reports_changed_columns():
    d = D.compare([{"id": "a", "amount": 10, "n": 1}], [{"id": "a", "amount": 11, "n": 1}], TYPES, key=["id"])
    assert not d.ok
    assert len(d.changed) == 1 and d.changed[0].columns == ["amount"]
    assert not d.missing and not d.unexpected


def test_without_key_similar_rows_pair_up_as_changes():
    d = D.compare([{"id": "a", "amount": 10, "n": 1}], [{"id": "a", "amount": 11, "n": 1}], TYPES)
    assert len(d.changed) == 1 and d.changed[0].columns == ["amount"]


def test_missing_and_unexpected():
    d = D.compare([{"id": "a"}, {"id": "b"}], [{"id": "b"}, {"id": "c"}], TYPES, key=["id"])
    assert [r["id"] for r in d.missing] == ["a"]
    assert [r["id"] for r in d.unexpected] == ["c"]


def test_unlisted_columns_are_wildcards_per_row():
    expected = [{"id": "a", "n": 1, "amount": 10}, {"id": "b", "n": 2}]  # b does not care about amount
    actual = [{"id": "a", "n": 1, "amount": "10.00"}, {"id": "b", "n": 2, "amount": "99.00"}]
    assert D.compare(expected, actual, TYPES, key=["id"]).ok
    assert not D.compare([{"id": "b", "amount": 1}], actual[1:], TYPES).ok


def test_numeric_tolerance_global_and_per_column():
    e, a = [{"id": "a", "amount": 10.00}], [{"id": "a", "amount": "10.01"}]
    assert not D.compare(e, a, TYPES).ok
    assert D.compare(e, a, TYPES, tolerance=0.02).ok
    assert D.compare(e, a, TYPES, tolerance={"amount": 0.05}).ok
    assert not D.compare(e, a, TYPES, tolerance={"n": 1}).ok


def test_duplicates_are_a_multiset():
    d = D.compare([{"id": "a"}, {"id": "a"}], [{"id": "a"}], TYPES)
    assert len(d.missing) == 1 and d.matched == 1


def test_ordered_comparison():
    e = [{"id": "a"}, {"id": "b"}]
    assert D.compare(e, e[::-1], TYPES).ok
    d = D.compare(e, e[::-1], TYPES, ordered=True)
    assert not d.ok and "different order" in d.order_problem


def test_ignore_columns():
    assert D.compare([{"id": "a", "n": 1}], [{"id": "a", "n": 2}], TYPES, ignore=["n"]).ok


def test_materialize_retract_changelog():
    names = ["id", "n"]
    changes = [{"kind": "+I", "values": ["a", 1]}, {"kind": "-U", "values": ["a", 1]},
               {"kind": "+U", "values": ["a", 2]}, {"kind": "+I", "values": ["b", 1]},
               {"kind": "-D", "values": ["b", 1]}]
    assert D.materialize(names, changes) == [{"id": "a", "n": 2}]


def test_materialize_upsert_by_key():
    names = ["id", "n"]
    changes = [{"kind": "+I", "values": ["a", 1]}, {"kind": "+U", "values": ["a", 2]},
               {"kind": "+I", "values": ["b", 1]}, {"kind": "-D", "values": ["b", 9]}]
    assert D.materialize(names, changes, ["id"]) == [{"id": "a", "n": 2}]


def test_renderers():
    d = D.compare([{"id": "a", "amount": 10}, {"id": "x"}], [{"id": "a", "amount": 11}, {"id": "y"}], TYPES,
                  key=["id"])
    text = D.render_text(d)
    assert "~ id=a" in text and "amount: expected 10, got 11" in text
    md = D.render_markdown(d)
    assert "| id=a | `amount` | 10 | 11 |" in md
    js = D.to_json(d)
    assert js["changed"] == 1 and js["missing"] == 1 and js["unexpected"] == 1
    assert js["changed_samples"][0]["columns"]["amount"] == {"expected": "10", "actual": "11"}


def test_large_keyed_diff_is_fast():
    import time
    n = 50_000
    base = [{"id": str(i), "amount": f"{i}.00", "n": i} for i in range(n)]
    cand = [dict(r) for r in base]
    cand[123]["amount"] = "0.01"
    t0 = time.time()
    d = D.compare(base, cand, TYPES, key=["id"], tolerance=Decimal("0.001"))
    assert time.time() - t0 < 10
    assert len(d.changed) == 1 and d.matched == n - 1
