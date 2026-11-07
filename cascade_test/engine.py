"""The test engine: one long-lived JVM with a shared MiniCluster (see java/src/.../Engine.java).

Starting Flink is the slow part of a pipeline test. The engine starts once per session (a CLI run,
a pytest session, a watch loop) and then runs every test as a job on the same cluster, several at
a time. The engine jar is built on first use against the Flink distribution in FLINK_HOME and
cached per Flink version, so there is no separate build step.
"""
from __future__ import annotations

import atexit
import glob
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time

PREFIX = "@@cascade-test "
HERE = os.path.dirname(os.path.abspath(__file__))
JAVA_DIR = os.path.join(HERE, "java")


class EngineError(RuntimeError):
    pass


def cache_dir() -> str:
    d = os.environ.get("CASCADE_TEST_CACHE") or os.path.join(
        os.environ.get("XDG_CACHE_HOME") or os.path.expanduser("~/.cache"), "cascade-test")
    os.makedirs(d, exist_ok=True)
    return d


def find_flink_home(explicit: str | None = None) -> str | None:
    from cascade_dsl.jvm import find_flink_home as find
    return find(explicit)


def dsl_jars() -> tuple[str, str]:
    """cascade-dsl's runtime jar (expectation functions used by generated SQL) and tools jar (the
    `cascade-fixture` source connector)."""
    from cascade_dsl import jvm
    tools = os.path.join(jvm.TARGET, "cascade-tools.jar")
    runtime = os.path.join(jvm.TARGET, "cascade-runtime.jar")
    if not (os.path.exists(tools) and os.path.exists(runtime)):
        raise EngineError("cascade-dsl's JVM jars are not built; run: "
                          f"FLINK_HOME=<flink> {os.path.join(os.path.dirname(jvm.TARGET), 'build.sh')}")
    return runtime, tools


def _sources() -> list[str]:
    return sorted(glob.glob(os.path.join(JAVA_DIR, "src", "**", "*.java"), recursive=True))


def _fingerprint(flink_home: str) -> str:
    h = hashlib.sha256()
    for path in _sources() + sorted(glob.glob(os.path.join(JAVA_DIR, "resources", "**", "*"), recursive=True)):
        if os.path.isfile(path):
            h.update(path.encode())
            with open(path, "rb") as f:
                h.update(f.read())
    for jar in sorted(glob.glob(os.path.join(flink_home, "lib", "flink-dist*.jar"))):
        h.update(os.path.basename(jar).encode())
        h.update(str(os.path.getsize(jar)).encode())
    return h.hexdigest()[:16]


def engine_jar(flink_home: str) -> str:
    """Build (once) and return the engine jar for this Flink distribution."""
    jar = os.path.join(cache_dir(), f"engine-{_fingerprint(flink_home)}.jar")
    if os.path.exists(jar):
        return jar
    javac = os.path.join(os.environ["JAVA_HOME"], "bin", "javac") if os.environ.get("JAVA_HOME") else "javac"
    jar_tool = os.path.join(os.path.dirname(javac), "jar") if os.environ.get("JAVA_HOME") else "jar"
    if shutil.which(javac) is None and not os.path.exists(javac):
        raise EngineError("javac not found: the test engine is compiled on first use (install a JDK 11+)")
    with tempfile.TemporaryDirectory(prefix="cascade-test-build-") as out:
        classes = os.path.join(out, "classes")
        os.makedirs(classes)
        cp = os.path.join(flink_home, "lib", "*")
        proc = subprocess.run([javac, "--release", "11", "-nowarn", "-cp", cp, "-d", classes, *_sources()],
                              capture_output=True, text=True)
        if proc.returncode != 0:
            raise EngineError(f"building the test engine against {flink_home} failed:\n{proc.stderr[-3000:]}")
        shutil.copytree(os.path.join(JAVA_DIR, "resources"), classes, dirs_exist_ok=True)
        tmp = jar + f".{os.getpid()}.tmp"
        proc = subprocess.run([jar_tool, "cf", tmp, "-C", classes, "."], capture_output=True, text=True)
        if proc.returncode != 0:
            raise EngineError(f"packaging the test engine failed:\n{proc.stderr}")
        os.replace(tmp, jar)
    return jar


class Engine:
    """A running engine JVM. Thread-safe: requests are serialized; the engine itself runs the tests
    of one request concurrently."""

    def __init__(self, flink_home: str | None = None, workers: int | None = None, slots: int | None = None,
                 startup_timeout: int = 180):
        home = find_flink_home(flink_home)
        if home is None:
            raise EngineError("Flink not found: set FLINK_HOME (or pass --flink-home) to a Flink 2.x distribution")
        self.flink_home = home
        self.workers = workers or max(2, min(8, (os.cpu_count() or 4) // 2))
        self.slots = slots or max(8, self.workers * 4)
        self._lock = threading.Lock()
        self._proc: subprocess.Popen | None = None
        self.info: dict = {}
        self.log_path = os.path.join(cache_dir(), f"engine-{os.getpid()}.log")
        self._start(startup_timeout)

    def _command(self) -> list[str]:
        from cascade_dsl import jvm
        runtime, tools = dsl_jars()
        jar = engine_jar(self.flink_home)
        t = jvm.Tools(self.flink_home)
        cp = [jar, tools, runtime, os.path.join(self.flink_home, "lib", "*")]
        cp += sorted(glob.glob(os.path.join(self.flink_home, "opt", "flink-python*.jar")))[:1]
        log4j = os.path.join(JAVA_DIR, "resources", "log4j2.properties")
        return [t._java(), *t._java_opts(), f"-Dlog4j.configurationFile={log4j}",
                f"-Dcascade.workers={self.workers}", f"-Dcascade.slots={self.slots}",
                "-XX:+UseParallelGC", "-XX:TieredStopAtLevel=1",
                "-cp", os.pathsep.join(cp), "dev.cascade.testengine.Engine"]

    def _start(self, timeout: int):
        env = dict(os.environ)
        from cascade_dsl.jvm import python_executable
        py = python_executable()
        if py:
            env["CASCADE_PYTHON"] = py
        self._log = open(self.log_path, "w")
        self._proc = subprocess.Popen(self._command(), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=self._log, text=True, env=env, bufsize=1)
        result: dict = {}

        def wait():
            try:
                result["info"] = self._read()
            except EngineError as e:
                result["error"] = e
        t = threading.Thread(target=wait, daemon=True)
        t.start()
        t.join(timeout)
        if "info" not in result:
            self.close()
            raise result.get("error") or EngineError(f"the test engine did not start within {timeout}s"
                                                     f"{self._log_tail()}")
        self.info = result["info"]

    def _read(self) -> dict:
        assert self._proc is not None
        while True:
            line = self._proc.stdout.readline()
            if not line:
                raise EngineError(f"the test engine exited (code {self._proc.poll()}){self._log_tail()}")
            if line.startswith(PREFIX):
                return json.loads(line[len(PREFIX):])

    def _log_tail(self, n: int = 40) -> str:
        try:
            self._log.flush()
            with open(self.log_path) as f:
                lines = [x for x in f.read().splitlines() if x.strip()]
            return ("\nengine log (" + self.log_path + "):\n  " + "\n  ".join(lines[-n:])) if lines else ""
        except OSError:
            return ""

    def request(self, payload: dict) -> dict:
        with self._lock:
            if self._proc is None or self._proc.poll() is not None:
                raise EngineError("the test engine is not running")
            self._proc.stdin.write(json.dumps(payload) + "\n")
            self._proc.stdin.flush()
            out = self._read()
        if "error" in out and len(out) == 1:
            raise EngineError(out["error"])
        return out

    def run(self, tests: list[dict]) -> list[dict]:
        if not tests:
            return []
        return self.request({"cmd": "run", "tests": tests})["results"]

    def describe(self, setup: list[str], config: dict | None = None) -> dict:
        return self.request({"cmd": "describe", "setup": setup, "config": config or {}})

    @property
    def alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def close(self):
        p, self._proc = self._proc, None
        if p is None:
            return
        try:
            if p.poll() is None:
                p.stdin.write('{"cmd": "shutdown"}\n')
                p.stdin.flush()
                p.wait(timeout=15)
        except Exception:
            pass
        if p.poll() is None:
            p.kill()
        try:
            self._log.close()
            if os.path.exists(self.log_path) and p.returncode == 0:
                os.remove(self.log_path)
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


_shared: Engine | None = None
_shared_lock = threading.Lock()


def shared_engine(flink_home: str | None = None) -> Engine:
    """A process-wide engine, started on first use and stopped at exit (Python API and pytest)."""
    global _shared
    with _shared_lock:
        if _shared is None or not _shared.alive:
            started = time.time()
            _shared = Engine(flink_home)
            _shared.info["python_startup_seconds"] = round(time.time() - started, 2)
            atexit.register(_shared.close)
        return _shared
