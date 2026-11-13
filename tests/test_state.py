"""Savepoint inspection and surgery (State Processor API), and replaying from a savepoint."""
import os
import shutil

import pytest

from cascade_test import cli, replay as RP
from cascade_test.engine import (Engine, EngineError, StateToolError, dsl_jars, find_flink_home,
                                 state_processor_jar, state_tool)

HERE = os.path.dirname(os.path.abspath(__file__))
SAVEPOINT = os.path.join(HERE, "fixtures", "savepoint-agg")
STATEFUL = ["e03d9229619566fd5d62dea3a8d25d1e", "570f707193e0fe32f4d86d067aba243b"]


def _available() -> bool:
    try:
        dsl_jars()
    except EngineError:
        return False
    home = find_flink_home()
    return home is not None and state_processor_jar(home) is not None


needs_flink = pytest.mark.skipif(not _available(), reason="needs FLINK_HOME (Flink 2.x with opt/flink-state-processor-api)")


# --- replay wiring (no Flink) ----------------------------------------------------------------------

class FakeEngine:
    flink_home = "/flink"


def test_restore_config_points_at_the_savepoint_and_tolerates_dropped_operators():
    warnings = []
    cfg, tmp = RP.restore_config(FakeEngine(), SAVEPOINT, [], [], warnings)
    assert tmp is None
    assert cfg == {"execution.state-recovery.path": SAVEPOINT,
                   "execution.state-recovery.ignore-unclaimed-state": "true"}
    assert warnings == [f"state: restored from savepoint {SAVEPOINT}"]


def test_restore_config_rejects_a_directory_that_is_not_a_savepoint(tmp_path):
    with pytest.raises(RP.ReplayError, match="not a savepoint"):
        RP.restore_config(FakeEngine(), str(tmp_path), [], [], [])


def test_restore_config_subsets_a_copy(monkeypatch):
    seen = {}

    def fake(home, args):
        seen["args"] = args
        return {"removed": ["a"], "kept": ["b", "c"], "not_in_savepoint": ["nope"]}
    monkeypatch.setattr("cascade_test.engine.state_tool", fake)
    warnings = []
    cfg, tmp = RP.restore_config(FakeEngine(), SAVEPOINT, ["a", "nope"], [], warnings)
    try:
        assert seen["args"][0] == "subset" and seen["args"][3:] == ["--drop", "a", "--drop", "nope"]
        assert cfg["execution.state-recovery.path"] == seen["args"][2] and os.path.isdir(tmp)
        assert warnings == ["state: 'nope' is not an operator of the savepoint",
                            "state: restored from a copy of the savepoint without 1 operator(s)' state (2 kept)"]
    finally:
        shutil.rmtree(tmp)


def test_state_options_need_a_savepoint():
    with pytest.raises(RP.ReplayError, match="need --from-savepoint"):
        RP.replay(FakeEngine(), None, None, state_drop=["a"])


# --- the State Processor API tool (real Flink) -------------------------------------------------------

@needs_flink
def test_inspect_lists_operators_and_names_known_uids():
    res = state_tool(find_flink_home(), ["inspect", SAVEPOINT, "--uid", "not-an-operator"])
    assert len(res["operators"]) == 8
    stateful = [o["id"] for o in res["operators"] if o["state_bytes"] > 0]
    assert sorted(stateful) == sorted(STATEFUL)
    assert res["state_bytes"] == sum(o["state_bytes"] for o in res["operators"]) > 20_000
    assert res["uids_not_in_savepoint"] == ["not-an-operator"]


@needs_flink
def test_subset_drops_and_keeps_whole_operators(tmp_path):
    home = find_flink_home()
    dropped = state_tool(home, ["subset", SAVEPOINT, str(tmp_path / "d"), "--drop", STATEFUL[0], "--drop", "ghost"])
    assert dropped["removed"] == [STATEFUL[0]] and len(dropped["kept"]) == 7 and dropped["not_in_savepoint"] == ["ghost"]
    after = state_tool(home, ["inspect", str(tmp_path / "d")])
    assert [o["id"] for o in after["operators"] if o["state_bytes"] > 0] == [STATEFUL[1]]

    kept = state_tool(home, ["subset", SAVEPOINT, str(tmp_path / "k"), "--keep", STATEFUL[1]])
    assert kept["kept"] == [STATEFUL[1]] and len(kept["removed"]) == 7
    assert state_tool(home, ["inspect", str(tmp_path / "k")])["state_bytes"] == after["state_bytes"]


@needs_flink
def test_subset_refuses_bad_requests(tmp_path):
    home = find_flink_home()
    with pytest.raises(StateToolError, match="--drop .* or --keep"):
        state_tool(home, ["subset", SAVEPOINT, str(tmp_path / "x")])
    (tmp_path / "there").mkdir()
    with pytest.raises(StateToolError, match="already exists"):
        state_tool(home, ["subset", SAVEPOINT, str(tmp_path / "there"), "--drop", STATEFUL[0]])


@needs_flink
def test_cli_state_commands(tmp_path, capsys):
    assert cli.main(["state", "inspect", SAVEPOINT]) == 0
    assert "e03d9229619566fd5d62dea3a8d25d1e" in capsys.readouterr().out
    out = str(tmp_path / "out")
    assert cli.main(["state", "subset", SAVEPOINT, out, "--drop", STATEFUL[0]]) == 0
    assert "7 operator(s) kept, 1 removed" in capsys.readouterr().out
    assert cli.main(["state", "inspect", str(tmp_path)]) == 2
    assert "not a savepoint" in capsys.readouterr().err


# --- a job restoring from the savepoint ---------------------------------------------------------------

GEN = ("CREATE TEMPORARY TABLE g (k STRING, v BIGINT) WITH ('connector'='datagen','number-of-rows'='20',"
       "'rows-per-second'='1000','fields.k.length'='2','fields.v.min'='1','fields.v.max'='9')")


def _job(name, config):
    return {"id": name, "timeout": 60, "config": {}, "setup": [f"SET '{k}' = '{v}'" for k, v in config.items()] + [GEN],
            "outputs": [{"name": "o", "query": "SELECT k, SUM(v) AS s FROM g GROUP BY k"}]}


@needs_flink
def test_a_job_really_restores_from_the_savepoint_and_replay_tolerates_unmatched_state():
    """Without ignore-unclaimed-state Flink refuses (the savepoint's operators are not in this job), which
    shows the savepoint is read; with replay's configuration the job runs."""
    with Engine() as engine:
        cfg, _ = RP.restore_config(engine, SAVEPOINT, [], [], [])
        strict = {"execution.state-recovery.path": cfg["execution.state-recovery.path"]}
        strict_res, tolerant_res = engine.run([_job("strict", strict), _job("tolerant", cfg)])
    assert "Cannot map checkpoint/savepoint state for operator" in strict_res["error"]
    assert "error" not in tolerant_res and tolerant_res["outputs"]["o"]["rows"]
