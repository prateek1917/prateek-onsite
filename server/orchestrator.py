from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
from client.workflow import Workflow, TaskSpec  # In practice would be getting Workflow info via serialized instead of importing client 
from func_registry import get as registry_get  # In practice would be done through a DB

class DependencyResolver:
    def __init__(self, wf: Workflow):
        self.task_index: Dict[str, TaskSpec] = dict(wf._tasks)  # id -> spec
        self.graph = defaultdict(list)
        self.indegree = defaultdict(int)
        for t in self.task_index.values():
            for d in t.deps:
                self.graph[d].append(t.task_id)
                self.indegree[t.task_id] += 1

    def tasks(self):
        return list(self.task_index.values())

    def initial_ready(self) -> List[str]:
        return [t.task_id for t in self.task_index.values() if self.indegree[t.task_id] == 0]

    def successors(self, task_id: str) -> List[str]:
        return self.graph[task_id]

    def task_of(self, task_id: str) -> TaskSpec:
        return self.task_index[task_id]

class Scheduler:
    def __init__(self):
        self.q: deque[str] = deque()

    def add_ready(self, task_ids: List[str]): 
        self.q.extend(task_ids)

    def next(self) -> Optional[str]:
        return self.q.popleft() if self.q else None

class Executor:
    def execute(self, func_ref: str, ctx: dict, params: Dict[str, Any]):
        fn = registry_get(func_ref)
        return fn(ctx, **params)

class Orchestrator:
    def run(self, workflow: Workflow, initial_ctx: Optional[dict] = None) -> dict:
        resolver = DependencyResolver(workflow)
        sched, exec_ = Scheduler(), Executor()

        ctx: Dict[str, Any] = dict(initial_ctx or {})
        ctx.setdefault("by_uuid", {})
        ctx.setdefault("by_name", defaultdict(list))

        indegree = dict(resolver.indegree)
        done: Dict[str, str] = {}

        sched.add_ready(resolver.initial_ready())

        while True:
            tid = sched.next()
            if tid is None:
                if len(done) == len(resolver.tasks()):
                    break
                pending = [t for t in resolver.tasks() if t.task_id not in done]
                raise RuntimeError(f"Deadlock or cycle: {[p.name for p in pending]}")

            t = resolver.task_of(tid)
            try:
                out = exec_.execute(t.func_ref, ctx, t.params)
                ctx["by_uuid"][tid] = out
                ctx["by_name"][t.name].append(out)
                ctx[f"task:{tid}:result"] = out
                done[tid] = "SUCCESS"
            except Exception as e:
                done[tid] = "FAILED"
                raise RuntimeError(f"Task {t.name} failed: {e}") from e

            for succ in resolver.successors(tid):
                indegree[succ] -= 1
                if indegree[succ] == 0:
                    sched.add_ready([succ])

        ctx["task_state"] = done
        return ctx
