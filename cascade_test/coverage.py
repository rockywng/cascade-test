"""Pipeline test coverage (`--coverage`).

Line coverage means little for SQL. What matters for a pipeline is:
  - models:        is each model's output asserted by some test (directly, through a sink, or via its
                   quarantine), or at least exercised upstream of one?
  - sinks:         is each sink's real INSERT (changelog mode, upsert key, ON CONFLICT) tested?
  - expectations:  has each data-quality check actually caught a bad row in some test? A check that
                   never fires in tests is a check nobody has seen work.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .project import Project
from .spec import PIPELINE


@dataclass
class Coverage:
    models: dict = field(default_factory=dict)  # model -> asserted | exercised | none
    sinks: dict = field(default_factory=dict)  # sink -> tested | none
    expectations: dict = field(default_factory=dict)  # model.expectation -> violations seen (int)

    def _pct(self, items, good) -> float:
        return 100.0 * sum(1 for v in items.values() if good(v)) / len(items) if items else 100.0

    @property
    def model_pct(self) -> float:
        return self._pct(self.models, lambda v: v == "asserted")

    @property
    def sink_pct(self) -> float:
        return self._pct(self.sinks, lambda v: v == "tested")

    @property
    def expectation_pct(self) -> float:
        return self._pct(self.expectations, lambda v: v > 0)

    @property
    def total_pct(self) -> float:
        items = len(self.models) + len(self.sinks) + len(self.expectations)
        if not items:
            return 100.0
        good = sum(1 for v in self.models.values() if v == "asserted") + \
            sum(1 for v in self.sinks.values() if v == "tested") + sum(1 for v in self.expectations.values() if v > 0)
        return 100.0 * good / items

    def text(self) -> str:
        lines = [f"coverage: {self.total_pct:.0f}%  (models asserted {self.model_pct:.0f}%, sinks tested "
                 f"{self.sink_pct:.0f}%, expectations exercised {self.expectation_pct:.0f}%)"]
        for m, v in self.models.items():
            mark = {"asserted": "✓", "exercised": "~", "none": "✗"}[v]
            note = {"asserted": "", "exercised": "  only exercised upstream of other targets",
                    "none": "  not tested"}[v]
            lines.append(f"  {mark} model {m}{note}")
        for k, v in self.sinks.items():
            lines.append(f"  {'✓' if v == 'tested' else '✗'} sink  {k}" + ("" if v == "tested" else "  not tested"))
        for k, v in self.expectations.items():
            lines.append(f"  {'✓' if v else '✗'} expectation {k}" +
                         (f"  ({v} violating row(s) seen)" if v else "  never violated by any test"))
        return "\n".join(lines)

    def markdown(self) -> str:
        rows = [f"#### Coverage: {self.total_pct:.0f}%", "", "| | item | status |", "|---|---|---|"]
        for m, v in self.models.items():
            rows.append(f"| {'✅' if v == 'asserted' else ('🟡' if v == 'exercised' else '❌')} | model `{m}` | {v} |")
        for k, v in self.sinks.items():
            rows.append(f"| {'✅' if v == 'tested' else '❌'} | sink `{k}` | {v} |")
        for k, v in self.expectations.items():
            rows.append(f"| {'✅' if v else '❌'} | expectation `{k}` | "
                        f"{f'{v} violation(s) seen' if v else 'never violated'} |")
        return "\n".join(rows) + "\n"

    def to_json(self) -> dict:
        return {"total_pct": round(self.total_pct, 1), "models": self.models, "sinks": self.sinks,
                "expectations": self.expectations}


def compute(project: Project, results) -> Coverage:
    p = project.pipeline
    cov = Coverage()
    asserted, exercised, sinks = set(), set(), set()
    seen: dict = {}
    for r in results:
        if r.status == "skip" or r.prep is None or r.case.kind == "calls":
            continue
        targets = list(r.prep.targets)
        if r.case.target == PIPELINE:
            targets = list((r.case.expect.get("sinks") or {}).keys())
            exercised.update(p.models)
        for t in targets:
            if t in p.sinks:
                sinks.add(t)
            rel = project.relation_of(t)
            asserted.add(rel)
            exercised.update(project.upstream(rel))
        for m, counts in r.expectation_counts.items():
            for name, n in counts.items():
                seen[f"{m}.{name}"] = seen.get(f"{m}.{name}", 0) + n
        # a test that expects a `fail` expectation to stop the job exercises that expectation
        wanted = str(r.case.expect.get("error") or "")
        if r.status == "pass" and wanted and r.job_error:
            for m in p.models.values():
                for e in m.expectations:
                    if e.on_violation == "fail" and e.name in wanted and e.name in r.job_error:
                        seen[f"{m.name}.{e.name}"] = seen.get(f"{m.name}.{e.name}", 0) + 1
    for m in project.checked.order:
        if p.models[m].copy_of:
            continue
        cov.models[m] = "asserted" if m in asserted else ("exercised" if m in exercised else "none")
        for e in p.models[m].expectations:
            cov.expectations[f"{m}.{e.name}"] = seen.get(f"{m}.{e.name}", 0)
    for s in p.sinks:
        cov.sinks[s] = "tested" if s in sinks else "none"
    return cov
