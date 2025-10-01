import uuid
from typing import Dict, Callable, List, Optional
from wf_types import TaskSpec
from func_registry import register

class Workflow:
    def __init__(self, name: str = "Workflow"):
        self.name = name
        self._tasks: Dict[str, TaskSpec] = {}

    def task(self, fn: Callable, possible_branches: Optional[List[Callable]] = None) -> str:
        """
        Register a task. Can be static or dynamic based on possible_branches.

        Args:
            fn: Function to execute
            possible_branches: Optional list of functions that can be registered at runtime.
                              If None, this is a regular task.
                              If provided, fn should return Callable or List[Callable].

        Returns:
            task_id

        Examples:
            # static task
            t_load = wf.task(load)

            # Dynamic task (branching)
            t_eval = wf.task(evaluate, possible_branches=[process_high, process_low])
        """
        if possible_branches is None:
            # Regular task
            task_id = str(uuid.uuid4())
            register(fn.__name__, fn)
            self._tasks[task_id] = TaskSpec(
                task_id=task_id,
                func_ref=fn.__name__,
                deps=[]
            )
            return task_id
        else:
            # Dynamic task
            return self._create_dynamic_task(fn, possible_branches)

    def link(self, upstream_id: str, downstream_id: str):
        """
        Defines downstream task id depends on upstream task id
        """
        self._tasks[downstream_id].deps.append(upstream_id)

    def get_task(self, fn: Callable) -> Optional[str]:
        """
        Get task_id for a given function.

        Args:
            fn: Function to find

        Returns:
            task_id if found, None otherwise
        """
        for tid, spec in self._tasks.items():
            if spec.func_ref == fn.__name__:
                return tid
        return None


    def _create_dynamic_task(self, fn: Callable, possible_branches: List[Callable]) -> str:
        """Internal: Create a task that dynamically spawns other tasks"""
        # Register all possible branch tasks
        branch_map = {}
        for branch_fn in possible_branches:
            # Check if already registered
            existing_id = None
            for tid, spec in self._tasks.items():
                if spec.func_ref == branch_fn.__name__:
                    existing_id = tid
                    break

            if existing_id:
                branch_map[branch_fn.__name__] = existing_id
            else:
                branch_id = self.task(branch_fn)
                branch_map[branch_fn.__name__] = branch_id

        # Wrapper that calls user function and registers branches
        user_fn = fn
        def wrapper(ctx):
            result = user_fn()
            # Handle both single function and list of functions
            to_register = result if isinstance(result, list) else [result]
            branch_labels = [branch_fn.__name__ for branch_fn in to_register]
            ctx.register_branches(branch_labels)

        # Register wrapper
        task_id = str(uuid.uuid4())
        register(fn.__name__, wrapper)
        self._tasks[task_id] = TaskSpec(
            task_id=task_id,
            func_ref=fn.__name__,
            deps=[],
            dynamic_spawns=branch_map
        )
        return task_id

    def map_reduce(self, mapper_task_id: str, reducer_task_id: str, input_data: List):
        pass
