import textwrap

import pytest

from cascade_test import spec as S


def load(tmp_path, text):
    p = tmp_path / "tests"
    p.mkdir(exist_ok=True)
    f = p / "t.yaml"
    f.write_text(textwrap.dedent(text))
    return S.load_file(str(f))


def test_existing_cascade_dsl_files_still_load(tmp_path):
    cases = load(tmp_path, """
        tests:
          - name: ltv
            target: customer_ltv
            given: {orders: [{order_id: o1}]}
            expect: {rows: [{customer_id: c1}]}
            ordered: false
            watermarks: per_row
          - name: none
            target: clean_orders
            expect: {empty: true}
    """)
    assert [c.name for c in cases] == ["ltv", "none"]
    assert cases[1].expect["rows"] == []


def test_parameters_expand_into_named_cases(tmp_path):
    cases = load(tmp_path, """
        tests:
          - name: drops
            target: clean_orders
            parameters:
              zero: {amount: 0}
              negative: {amount: -1}
            given: {orders: [{amount: "${amount}", note: "amount is ${amount}"}]}
            expect: {count: 0}
    """)
    assert [c.name for c in cases] == ["drops[zero]", "drops[negative]"]
    assert cases[1].given["orders"][0] == {"amount": -1, "note": "amount is -1"}


def test_use_merges_named_fixtures_before_given(tmp_path):
    cases = load(tmp_path, """
        fixtures:
          base: {customers: [{customer_id: c1}], orders: [{order_id: o1}]}
        tests:
          - name: t
            target: x
            use: [base]
            given: {orders: [{order_id: o2}]}
            expect: {count: 1}
    """)
    assert cases[0].given["orders"] == [{"order_id": "o1"}, {"order_id": "o2"}]
    assert cases[0].given["customers"] == [{"customer_id": "c1"}]


def test_defaults_apply_and_tests_override(tmp_path):
    cases = load(tmp_path, """
        defaults:
          base_time: "2026-01-01 10:00:00"
          rows: {orders: {currency: usd}}
          tolerance: 0.01
        tests:
          - name: t
            target: x
            defaults: {orders: {amount: 1}}
            tolerance: 0.5
            expect: {count: 1}
    """)
    c = cases[0]
    assert c.base_time == "2026-01-01 10:00:00"
    assert c.defaults["orders"] == {"currency": "usd", "amount": 1}
    assert c.tolerance == 0.5


@pytest.mark.parametrize("text,message", [
    ("tests: [{name: t, target: x, expct: {count: 1}}]", "did you mean 'expect'"),
    ("tests: [{name: t, target: x, expect: {rowz: []}}]", "did you mean 'rows'"),
    ("tests: [{name: t, target: x}]", "has no `expect`"),
    ("tests: [{name: t, expect: {count: 1}}]", "needs a `target`"),
    ("tests: [{name: t, target: pipeline, expect: {count: 1}}]", "expects per sink"),
    ("tests: [{name: t, target: x, use: [nope], expect: {count: 1}}]", "unknown fixture"),
    ("tests: [{name: t, target: x, watermarks: sometimes, expect: {count: 1}}]", "watermarks must be"),
    ("tests: [{name: t, target: x, expect: {count: 1}}, {name: t, target: x, expect: {count: 1}}]", "duplicate"),
    ("tests: [{name: t, target: x, parameters: [{a: 1}], given: {o: [{v: '${b}'}]}, expect: {count: 1}}]",
     "unknown parameter"),
])
def test_spec_errors_point_at_the_problem(tmp_path, text, message):
    with pytest.raises(S.SpecError, match=message):
        load(tmp_path, text)


def test_select_by_name_tag_and_only(tmp_path):
    cases = load(tmp_path, """
        tests:
          - {name: fast_one, target: x, tags: [fast], expect: {count: 1}}
          - {name: slow_one, target: x, tags: [slow], expect: {count: 1}}
          - {name: other, target: x, expect: {count: 1}}
    """)
    assert [c.name for c in S.select(cases, "one")] == ["fast_one", "slow_one"]
    assert [c.name for c in S.select(cases, tags=["slow"])] == ["slow_one"]
    assert [c.name for c in S.select(cases, tags=["!slow"])] == ["fast_one", "other"]
    cases[2].only = True
    assert [c.name for c in S.select(cases)] == ["other"]


def test_snapshot_files_are_not_test_files(tmp_path):
    (tmp_path / "tests" / "__snapshots__").mkdir(parents=True)
    (tmp_path / "tests" / "__snapshots__" / "a.snap.yaml").write_text("x: 1\n")
    (tmp_path / "tests" / "a.yaml").write_text("tests: []\n")
    assert [p.split("/")[-1] for p in S.test_files(str(tmp_path))] == ["a.yaml"]
