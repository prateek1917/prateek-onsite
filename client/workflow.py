import uuid
from typing import Dict, Callable, List, Optional
from wf_types import TaskSpec
from func_registry import register

class Workflow:
    def __init__(self, name: str = "Workflow"):
        self.name = name
        self._tasks: Dict[str, TaskSpec] = {}

    def task(self, fn: Callable) -> str:
        """
        Register function. Takes function ptr and return task_id
        """
        task_id = str(uuid.uuid4())
        register(fn.__name__, fn)
        self._tasks[task_id] = TaskSpec(
            task_id=task_id,
            func_ref=fn.__name__,
            deps=[]
        )
        return task_id

    def link(self, upstream_id: str, downstream_id: str):
        """
        Defines downstream task id depends on upstream task id
        """
        self._tasks[downstream_id].deps.append(upstream_id)

    def branching(self, upstream_id: str, branch_specs: Dict[str, str]):
        """
        Declares that upstream_id is a branching task that MAY spawn tasks
        based on boolean conditions it evaluates at runtime.

        Args:
            upstream_id: Task that will evaluate conditions
            branch_specs: {"label": task_id} mapping of possible branches

        Example:
            wf.branching(t_eval, {"high": t_a, "low": t_b})

            # Task evaluates conditions and spawns:
            def evaluate(ctx):
                value = compute_something()
                if value > 10:  # Boolean expression
                    ctx.spawn_branch("high")
                else:
                    ctx.spawn_branch("low")
        """
        if upstream_id not in self._tasks:
            raise ValueError(f"Task {upstream_id} not found in workflow")

        for task_id in branch_specs.values():
            if task_id not in self._tasks:
                raise ValueError(f"Task {task_id} not found in workflow")

        self._tasks[upstream_id].dynamic_spawns = branch_specs

    def map_reduce(self, mapper_task_id: str, reducer_task_id: str, input_data: List):
        pass
