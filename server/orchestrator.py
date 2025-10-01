from typing import Dict, List, Optional, Any
from collections import defaultdict, deque
from client.workflow import Workflow  # in practice would be serialized and sent over proto
from wf_types import TaskSpec, Constraint
from func_registry import get as registry_get  # In practice would be done through a DB


class ExecutionContext:
    """Context passed to tasks to register which branches should execute at runtime"""

    def __init__(self, orchestrator: 'Orchestrator', current_task_id: str, task_spec: TaskSpec):
        self._orchestrator = orchestrator
        self._current_task_id = current_task_id
        self._task_spec = task_spec
        self._registered_branches: List[str] = []

    def register_branches(self, branch_labels: List[str]):
        """
        Register which branches should execute at runtime.
        All possible branches already exist in the DAG (compile-time).
        This marks which ones should actually run.

        Args:
            branch_labels: List of function names to register for execution
        """
        if self._task_spec.dynamic_spawns is None:
            raise RuntimeError(f"Task {self._current_task_id} is not a dynamic task")

        for label in branch_labels:
            if label not in self._task_spec.dynamic_spawns:
                raise RuntimeError(
                    f"Task {self._current_task_id} cannot register branch '{label}'. "
                    f"Allowed branches: {list(self._task_spec.dynamic_spawns.keys())}"
                )
            self._registered_branches.append(label)

    def get_registered_branches(self) -> List[str]:
        """Get list of branch labels that were registered"""
        return self._registered_branches

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

        # Transitive: tasks whose ALL dependencies are dynamic
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


class ConstraintValidator:
    """Validates task constraints before execution"""

    def validate_before_execution(self, task: TaskSpec):
        """
        Validate all constraints on a task before it executes.

        Args:
            task: TaskSpec to validate

        Raises:
            RuntimeError: If any constraint is violated
        """
        for constraint in task.constraints:
            if constraint == Constraint.STATIC:
                self._validate_static(task)

    def _validate_static(self, task: TaskSpec):
        """
        Validate STATIC constraint: task cannot be a dynamic task.

        Args:
            task: TaskSpec to validate

        Raises:
            RuntimeError: If task has dynamic_spawns
        """
        if task.dynamic_spawns is not None:
            raise RuntimeError(
                f"Constraint violation: Task '{task.func_ref}' has STATIC constraint "
                f"but is a dynamic task"
            )


class Orchestrator:
    def __init__(self):
        self.resolver: Optional[DependencyResolver] = None
        self.sched: Optional[Scheduler] = None
        self.indegree: Dict[str, int] = {}
        self.done: Dict[str, str] = {}
        self.blocked_branches: Dict[str, int] = {}  # task_id -> actual indegree (saved for later)

    def _register_branches_for_execution(self, ctx: ExecutionContext):
        """
        Enable registered branches for execution by restoring their actual indegree.
        Called after a dynamic task completes.

        Args:
            ctx: ExecutionContext containing registered branches
        """
        registered = ctx.get_registered_branches()
        task_spec = ctx._task_spec

        for label in registered:
            task_id = task_spec.dynamic_spawns[label]

            if task_id in self.done:
                # Already executed, skip
                continue

            # Restore actual indegree (unblock the branch)
            if task_id in self.blocked_branches:
                self.indegree[task_id] = self.blocked_branches[task_id]

                # Check if ready to execute
                if self.indegree[task_id] == 0:
                    self.sched.add_ready([task_id])

    def run(self, workflow: Workflow) -> Dict[str, str]:
        self.resolver = DependencyResolver(workflow)
        self.sched = Scheduler()
        exec_ = Executor()

        self.indegree = dict(self.resolver.indegree)
        self.done = {}
        self.blocked_branches = {}

        # Block all dynamic-only tasks (infinite indegree) until registered at runtime
        for task_id in self.resolver.dynamic_only_tasks:
            # Save actual indegree for later restoration
            self.blocked_branches[task_id] = self.indegree.get(task_id, 0)
            # Block by setting to infinity
            self.indegree[task_id] = float('inf')

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

            # Validate constraints before execution
            validator = ConstraintValidator()
            validator.validate_before_execution(t)

            try:
                exec_.execute(t.func_ref, ctx)
                self.done[tid] = "SUCCESS"

                # If this is a dynamic task, register its branches for execution
                if t.dynamic_spawns is not None:
                    self._register_branches_for_execution(ctx)

            except Exception as e:
                self.done[tid] = "FAILED"
                raise RuntimeError(f"Task {t.func_ref} failed: {e}") from e

            for succ in self.resolver.successors(tid):
                # Skip if successor has infinite indegree (blocked branch)
                if self.indegree[succ] == float('inf'):
                    continue
                self.indegree[succ] -= 1
                if self.indegree[succ] == 0:
                    self.sched.add_ready([succ])

        return self.done
