"""`cascade-test state`: look inside a savepoint, and cut operators' state out of a copy of it.

Built on Flink's State Processor API (see java/.../StateTool.java). The unit is an operator, not a key:
a SQL operator's keyed state is serialised with types generated for that operator, so rows can't be
filtered without its code, but a whole operator's state can be removed. Use it to

  * see which operators hold how much state before a replay or an upgrade;
  * replay from a production savepoint without carrying over operators you are changing (their state
    would otherwise restore into a changed query), or with only the operator you are testing.

Operators are named by their pinned UID (what the compiler writes into the plan) or by the 32-character
hex operator id that `inspect` prints.
"""
from __future__ import annotations

import json
import os
import sys

from .engine import StateToolError, find_flink_home, state_tool


def _human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def savepoint_path(path: str) -> str:
    p = os.path.abspath(path)
    if not os.path.isfile(os.path.join(p, "_metadata")):
        raise StateToolError(f"{path} is not a savepoint directory (no _metadata file)")
    return p


def format_inspect(res: dict) -> str:
    out = [f"savepoint {res['savepoint']}  (checkpoint {res['checkpoint_id']}, {_human(res['state_bytes'])} of state)"]
    for o in sorted(res["operators"], key=lambda o: -o["state_bytes"]):
        name = o.get("uid") or o["id"]
        out.append(f"  {name:<36} {_human(o['state_bytes']):>10}  parallelism {o['parallelism']}"
                   f" (max {o['max_parallelism']}), state in {o['subtasks_with_state']} subtask(s)")
    if res.get("uids_not_in_savepoint"):
        out.append("not in the savepoint: " + ", ".join(res["uids_not_in_savepoint"]))
    return "\n".join(out)


def run(args) -> int:
    home = find_flink_home(args.flink_home)
    if home is None:
        print("Flink not found: set FLINK_HOME (or pass --flink-home)", file=sys.stderr)
        return 2
    try:
        if args.state_cmd == "inspect":
            res = state_tool(home, ["inspect", savepoint_path(args.savepoint)] +
                             [x for u in args.uid for x in ("--uid", u)])
            print(json.dumps(res, indent=2) if args.json else format_inspect(res))
        else:
            if bool(args.drop) == bool(args.keep):
                print("give --drop (remove these operators) or --keep (remove all others), one of them", file=sys.stderr)
                return 2
            res = state_tool(home, ["subset", savepoint_path(args.savepoint), os.path.abspath(args.out)] +
                             [x for u in args.drop for x in ("--drop", u)] + [x for u in args.keep for x in ("--keep", u)])
            if args.json:
                print(json.dumps(res, indent=2))
            else:
                print(f"wrote {res['output']}: {len(res['kept'])} operator(s) kept, {len(res['removed'])} removed")
                for m in res.get("not_in_savepoint") or []:
                    print(f"  warning: '{m}' is not an operator of the savepoint", file=sys.stderr)
    except StateToolError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    return 0
