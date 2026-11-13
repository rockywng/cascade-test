"""A compiled cascade project, plus the graph questions tests ask about it."""
from __future__ import annotations

import os
import re

from cascade_dsl.compiler import compile_pipeline
from cascade_dsl.diagnostics import Severity

from .fixtures import SourceInfo

# functions whose result depends on when (or how often) a row is processed, not on the input
NONDETERMINISTIC = re.compile(
    r"\b(CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|LOCALTIMESTAMP|LOCALTIME|NOW|PROCTIME|"
    r"CURRENT_ROW_TIMESTAMP|UNIX_TIMESTAMP\s*\(\s*\)|RAND|RAND_INTEGER|UUID)\b", re.IGNORECASE)
# what a frozen clock (clock.py) leaves nondeterministic: randomness and processing time
STILL_NONDETERMINISTIC = re.compile(r"\b(PROCTIME|RAND|RAND_INTEGER|UUID)\b", re.IGNORECASE)


class ProjectError(Exception):
    pass


class Project:
    def __init__(self, path: str, env: str | None = None):
        self.path = os.path.abspath(path)
        self.env = env
        r = compile_pipeline(path, env)
        if r.lowered is None:
            msgs = [d.format() for d in r.diagnostics.items if d.severity == Severity.ERROR]
            raise ProjectError(f"{path} does not compile:\n" + "\n".join(msgs or ["(no diagnostics)"]))
        self.result = r
        self.checked = r.checked
        self.pipeline = r.checked.pipeline
        self.lowered = r.lowered
        self.root = self.pipeline.root_dir
        self.warnings = [d.format() for d in r.diagnostics.items if d.severity == Severity.WARNING]
        self._views = {st.name for st in self.lowered.statements if st.kind == "view"}

    @property
    def name(self) -> str:
        return self.pipeline.name

    # --- graph --------------------------------------------------------------------------------

    def relations(self) -> list[str]:
        return list(self.pipeline.sources) + list(self.checked.order)

    def inputs(self, name: str) -> list[str]:
        rel = self.pipeline.relations.get(name)
        return list(rel.inputs) if rel else []

    def upstream(self, name: str) -> set[str]:
        """Every source and model `name` reads from, transitively (excluding itself)."""
        seen: set[str] = set()
        stack = list(self.inputs(name))
        while stack:
            n = stack.pop()
            if n in seen:
                continue
            seen.add(n)
            stack += self.inputs(n)
        return seen

    def downstream_models(self, name: str) -> set[str]:
        return {m for m in self.checked.order if name in self.upstream(m)}

    def relation_of(self, target: str) -> str:
        """The source/model a target's rows come from."""
        if target.endswith(".quarantined"):
            return target[: -len(".quarantined")]
        if target in self.pipeline.sinks:
            return self.pipeline.sinks[target].from_
        return target

    def view_of(self, target: str) -> str:
        if target.endswith(".quarantined"):
            return target[: -len(".quarantined")] + "__quarantined"
        if target in self.pipeline.sinks:
            return self.pipeline.sinks[target].from_
        return target

    def check_target(self, target: str) -> str | None:
        p = self.pipeline
        if target.endswith(".quarantined"):
            m = target[: -len(".quarantined")]
            if m not in p.models or not any(e.on_violation == "quarantine" for e in p.models[m].expectations):
                return f"target '{target}': '{m}' is not a model with quarantine expectations"
            return None
        if target in p.models or target in p.sources or target in p.sinks:
            return None
        from .fixtures import suggest
        options = list(p.models) + list(p.sinks) + list(p.sources) + ["pipeline"]
        return f"unknown target '{target}'{suggest(target, options)} (a model, <model>.quarantined, a sink or pipeline)"

    def key_of(self, target: str) -> list[str] | None:
        """The natural key of a target's rows: an upsert sink's primary key or a model's upsert key."""
        p = self.pipeline
        if target in p.sinks:
            s = p.sinks[target]
            return list(s.primary_key) if s.primary_key else None
        if target.endswith(".quarantined"):
            return None
        rel = p.relations.get(target)
        if rel and rel.upsert_key:
            return list(rel.upsert_key)
        if target in p.sources and p.sources[target].primary_key:
            return list(p.sources[target].primary_key)
        return None

    def materialize_key(self, target: str) -> list[str] | None:
        """Changes to an upsert sink are applied by primary key (what the external table holds)."""
        s = self.pipeline.sinks.get(target)
        return list(s.primary_key) if s is not None and s.mode == "upsert" and s.primary_key else None

    def source_info(self, name: str) -> SourceInfo:
        s = self.pipeline.sources[name]
        et = s.event_time
        return SourceInfo(name, [(c.name, c.type.text) for c in s.columns],
                          self.pipeline.relations[name].changelog == "updating", list(s.primary_key),
                          et.column if et else None, et.max_out_of_orderness_ms if et else 0)

    def expectation_base(self, model: str) -> str:
        """The view an expectation's check runs on (before filtering)."""
        return f"{model}__typed" if f"{model}__typed" in self._views else f"{model}__raw"

    def models_with_expectations(self, among) -> list[str]:
        return [m for m in self.checked.order if m in among and self.pipeline.models[m].expectations]

    def nondeterministic_columns(self, target: str, frozen: bool = False) -> list[str]:
        """Output columns computed from processing time or randomness (excluded from snapshots and
        replay diffs, where they would always differ). With `frozen` (the clock is frozen, clock.py),
        only the columns the frozen clock doesn't fix."""
        pattern = STILL_NONDETERMINISTIC if frozen else NONDETERMINISTIC
        rel = self.pipeline.relations.get(self.relation_of(target))
        cols = []
        if rel and rel.columns:
            for c in rel.columns:
                if c.expr and pattern.search(c.expr):
                    cols.append(c.name)
        if not frozen and (target.endswith(".quarantined") or target.endswith("__quarantine")):
            cols.append("_quarantined_at")
        for f in self.pipeline.functions.values():
            if not f.deterministic and rel and rel.columns:
                cols += [c.name for c in rel.columns if c.expr and re.search(rf"\b{re.escape(f.name)}\s*\(",
                                                                             c.expr, re.IGNORECASE)]
        return sorted(set(cols))

    def files(self) -> list[str]:
        """Files whose change should re-run tests (watch mode)."""
        out = []
        for d, dirs, names in os.walk(self.root):
            dirs[:] = [x for x in dirs if not x.startswith(".") and x not in ("target", "__pycache__", "__snapshots__")]
            for n in names:
                if n.endswith((".yaml", ".yml", ".sql", ".py", ".java", ".csv", ".jsonl", ".json")):
                    out.append(os.path.join(d, n))
        return out
