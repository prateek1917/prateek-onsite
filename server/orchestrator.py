from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
from client.workflow import Workflow  # in practice would be serialized and sent over proto
from wf_types import TaskSpec
from func_registry import get as registry_get  # In practice would be done through a DB


class ExecutionContext:
    """Context passed to tasks to allow dynamic tasks at runtime"""

    def __init__(self, orchestrator: 'Orchestrator', current_task_id: str, task_spec: TaskSpec):
        self._orchestrator = orchestrator
        self._current_task_id = current_task_id
        self._task_spec = task_spec

    def spawn_branch(self, label: str):
        """
        Spawn a task from the branching options.

        Args:
            label: Label from dynamic_spawns dict to spawn
        """
        if self._task_spec.dynamic_spawns is None:
            raise RuntimeError(f"Task {self._current_task_id} is not a branching task")

        if label not in self._task_spec.dynamic_spawns:
            raise RuntimeError(
                f"Task {self._current_task_id} cannot spawn branch '{label}'. "
                f"Allowed branches: {list(self._task_spec.dynamic_spawns.keys())}"
            )

        task_id = self._task_spec.dynamic_spawns[label]
        self._orchestrator.handle_spawn(self._current_task_id, task_id)

class DependencyResolver:
    def __init__(self, wf: Workflow):
        self.task_index: Dict[str, TaskSpec] = dict(wf._tasks)  # id -> spec
        self.graph = defaultdict(list)
        self.indegree = defaultdict(int)

        # Compute all dynamically spawnable tasks (including transitive)
        self.dynamic_only_tasks = self._compute_dynamic_only_tasks()

        for t in self.task_index.values():
            for d in t.deps:
                self.graph[d].append(t.task_id)
                self.indegree[t.task_id] += 1

    def _compute_dynamic_only_tasks(self) -> set:
        """
        Compute tasks that can only run if dynamically spawned.
        Includes both direct (in dynamic_spawns) and transitive
        (depends only on dynamic-only tasks).
        """
        dynamic_only = set()

        # Direct: tasks in someone's dynamic_spawns
        for t in self.task_index.values():
            if t.dynamic_spawns:
                dynamic_only.update(t.dynamic_spawns.values())

        # Transitive: tasks whose ALL dependencies are dynamic-only
        changed = True
        while changed:
            changed = False
            for t in self.task_index.values():
                if t.task_id in dynamic_only:
                    continue
                if not t.deps:
                    continue
                # If ALL deps are dynamic-only, this task is too
                if all(dep in dynamic_only for dep in t.deps):
                    dynamic_only.add(t.task_id)
                    changed = True

        return dynamic_only

    def tasks(self):
        return list(self.task_index.values())

    def initial_ready(self) -> List[str]:
        """Return tasks that are ready to execute initially 
        0 indegrees and not dynamically spawned"""
        return [
            t.task_id for t in self.task_index.values()
            if self.indegree[t.task_id] == 0 and t.task_id not in self.dynamic_only_tasks
        ]

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
    def execute(self, func_ref: str, ctx: ExecutionContext):
        fn = registry_get(func_ref)
        return fn(ctx)


class Orchestrator:
    def __init__(self):
        self.resolver: Optional[DependencyResolver] = None
        self.sched: Optional[Scheduler] = None
        self.indegree: Dict[str, int] = {}
        self.done: Dict[str, str] = {}

    def handle_spawn(self, spawner_id: str, new_task_id: str):
        """
        Called when a task spawns another task at runtime.

        Args:
            spawner_id: Task that is spawning
            new_task_id: Task to be spawned
        """
        if new_task_id in self.done:
            # Task already executed, skip
            return

        # Check if task has dependencies that aren't met
        new_task = self.resolver.task_of(new_task_id)
        deps_met = all(dep_id in self.done for dep_id in new_task.deps)

        if deps_met and self.indegree.get(new_task_id, 0) == 0:
            # Ready to execute
            self.sched.add_ready([new_task_id])

    def run(self, workflow: Workflow) -> Dict[str, str]:
        self.resolver = DependencyResolver(workflow)
        self.sched = Scheduler()
        exec_ = Executor()

        self.indegree = dict(self.resolver.indegree)
        self.done = {}

        self.sched.add_ready(self.resolver.initial_ready())

        while True:
            tid = self.sched.next()
            if tid is None:
                # Check if we're truly done
                pending = [t for t in self.resolver.tasks() if t.task_id not in self.done]

                # Filter out tasks that were never spawned (dynamic only tasks)
                pending_non_dynamic = [
                    t for t in pending
                    if t.task_id not in self.resolver.dynamic_only_tasks
                ]

                if not pending_non_dynamic:
                    # Only dynamic-only tasks remain, workflow complete
                    break

                raise RuntimeError(f"Deadlock or cycle: {[t.func_ref for t in pending_non_dynamic]}")

            t = self.resolver.task_of(tid)
            ctx = ExecutionContext(self, tid, t)

            try:
                exec_.execute(t.func_ref, ctx)
                self.done[tid] = "SUCCESS"
            except Exception as e:
                self.done[tid] = "FAILED"
                raise RuntimeError(f"Task {t.func_ref} failed: {e}") from e

            for succ in self.resolver.successors(tid):
                self.indegree[succ] -= 1
                if self.indegree[succ] == 0:
                    self.sched.add_ready([succ])

        return self.done
