"""A small DAG orchestrator: dependencies, parallel execution of independent tasks,
retries with exponential backoff, and skipping of tasks whose upstream failed."""
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Task:
    name: str
    fn: Callable
    deps: list[str] = field(default_factory=list)
    retries: int = 2           # extra attempts after the first
    retry_delay: float = 0.2   # seconds; doubles each retry


class Context:
    """Shared run state handed to every task."""

    def __init__(self, run_id, storage, config, results=None):
        self.run_id = run_id
        self.storage = storage
        self.config = config
        self.results: dict = results if results is not None else {}  # task name -> output
        self.stats: dict = {}                                        # free-form counters
        self.flaky_once: set = set()                                 # demo: tasks that fail once


def _topological_check(tasks: dict[str, Task]) -> None:
    for t in tasks.values():
        for d in t.deps:
            if d not in tasks:
                raise ValueError(f"Task '{t.name}' depends on unknown task '{d}'")
    state: dict[str, int] = {}

    def visit(n: str):
        if state.get(n) == 1:
            raise ValueError(f"Cycle detected at task '{n}'")
        if state.get(n) == 2:
            return
        state[n] = 1
        for d in tasks[n].deps:
            visit(d)
        state[n] = 2

    for n in tasks:
        visit(n)


class Orchestrator:
    def __init__(self, tasks: list[Task], max_workers: int = 4):
        self.tasks = {t.name: t for t in tasks}
        if len(self.tasks) != len(tasks):
            raise ValueError("Duplicate task names")
        _topological_check(self.tasks)
        self.max_workers = max_workers

    @staticmethod
    def _execute(task: Task, ctx: Context):
        start = time.perf_counter()
        error = None
        for attempt in range(1, task.retries + 2):
            try:
                out = task.fn(ctx)
                rows = len(out) if isinstance(out, (list, tuple)) else None
                return ({"task": task.name, "status": "SUCCESS", "attempts": attempt,
                         "duration_s": round(time.perf_counter() - start, 3), "rows": rows, "error": None}, out)
            except Exception as e:  # noqa: BLE001 - tasks may raise anything
                error = f"{type(e).__name__}: {e}"
                if attempt <= task.retries:
                    time.sleep(task.retry_delay * 2 ** (attempt - 1))
        return ({"task": task.name, "status": "FAILED", "attempts": task.retries + 1,
                 "duration_s": round(time.perf_counter() - start, 3), "rows": None, "error": error}, None)

    def run(self, ctx: Context) -> dict:
        pending = dict(self.tasks)
        status: dict[str, str] = {}
        records: dict[str, dict] = {}
        running: dict = {}
        started = time.perf_counter()

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            while pending or running:
                for name, t in list(pending.items()):
                    if any(status.get(d) in ("FAILED", "SKIPPED") for d in t.deps):
                        status[name] = "SKIPPED"
                        records[name] = {"task": name, "status": "SKIPPED", "attempts": 0,
                                         "duration_s": 0.0, "rows": None, "error": "upstream task failed"}
                        del pending[name]
                    elif all(status.get(d) == "SUCCESS" for d in t.deps):
                        running[ex.submit(self._execute, t, ctx)] = name
                        del pending[name]
                if not running:
                    break
                done, _ = wait(running, return_when=FIRST_COMPLETED)
                for fut in done:
                    name = running.pop(fut)
                    rec, out = fut.result()
                    status[name] = rec["status"]
                    records[name] = rec
                    if rec["status"] == "SUCCESS":
                        ctx.results[name] = out  # written from this thread only

        ordered = [records[n] for n in self.tasks if n in records]
        return {
            "run_id": ctx.run_id,
            "status": "SUCCESS" if all(r["status"] == "SUCCESS" for r in ordered) else "FAILED",
            "duration_s": round(time.perf_counter() - started, 3),
            "tasks": ordered,
            "stats": dict(ctx.stats),
        }
