import uuid
from typing import Dict, Callable, List, Optional
from wf_types import TaskSpec, Constraint
from func_registry import register

class Workflow:
    def __init__(self, name: str = "Workflow"):
        self.name = name
        self._tasks: Dict[str, TaskSpec] = {}

    def task(self, fn: Callable,
             possible_branches: Optional[List[Callable]] = None,
             constraints: Optional[List[Constraint]] = None) -> str:
        """
        Register a task. Can be static or dynamic based on possible_branches.

        Args:
            fn: Function to execute
            possible_branches: Optional list of functions that can be registered at runtime.
                              If None, this is a regular task.
                              If provided, fn should return Callable or List[Callable].
            constraints: Optional list of constraints to validate at runtime.

        Returns:
            task_id

        Examples:
            # static task
            t_load = wf.task(load)

            # Static task with STATIC constraint
            t_load = wf.task(load, constraints=[Constraint.STATIC])

            # Dynamic task (branching)
            t_eval = wf.task(evaluate, possible_branches=[process_high, process_low])
        """
        if constraints is None:
            constraints = []

        if possible_branches is None:
            # Regular task - wrap to hide ctx
            task_id = str(uuid.uuid4())

            # Only register wrapper if not already registered
            from func_registry import has
            if not has(fn.__name__):
                user_fn = fn
                def wrapper(ctx):
                    return user_fn()
                register(fn.__name__, wrapper)

            self._tasks[task_id] = TaskSpec(
                task_id=task_id,
                func_ref=fn.__name__,
                deps=[],
                constraints=constraints
            )
            return task_id
        else:
            # Dynamic task
            return self._create_dynamic_task(fn, possible_branches, constraints=constraints)

    def link(self, upstream_id: str, downstream_id: str):
        """
        Defines downstream task id depends on upstream task id
        """
        self._tasks[downstream_id].deps.append(upstream_id)

    def branched_task(self, fn: Callable, possible_branches: List[Callable]) -> str:
        """
        Create a branching task that must return exactly one branch.

        Args:
            fn: Function that returns Callable - exactly one function to execute
            possible_branches: List of all possible functions that could be returned

        Returns:
            task_id

        Example:
            def evaluate():
                if value > 10:
                    return process_high
                else:
                    return process_low

            t_eval = wf.branched_task(evaluate, [process_high, process_low])
        """
        return self._create_dynamic_task(fn, possible_branches, branching=True)

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

    def _create_dynamic_task(self, fn: Callable, possible_branches: List[Callable],
                             branching: bool = False, constraints: Optional[List[Constraint]] = None) -> str:
        """Internal: Create a task that dynamically spawns other tasks

        Args:
            fn: User function to execute
            possible_branches: List of possible branch functions
            branching: If True, enforce exactly one branch returned
            constraints: Optional list of constraints to validate at runtime
        """
        if constraints is None:
            constraints = []
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
        is_branching = branching
        def wrapper(ctx):
            result = user_fn()
            # Handle both single function and list of functions
            to_register = result if isinstance(result, list) else [result]

            # Enforce branching constraint: exactly one branch
            if is_branching and len(to_register) != 1:
                raise RuntimeError(
                    f"Branching task '{fn.__name__}' must return exactly one branch, "
                    f"but returned {len(to_register)} branches"
                )

            branch_labels = [branch_fn.__name__ for branch_fn in to_register]
            ctx.register_branches(branch_labels)

        # Register wrapper only if not already registered
        task_id = str(uuid.uuid4())
        from func_registry import has
        if not has(fn.__name__):
            register(fn.__name__, wrapper)
        self._tasks[task_id] = TaskSpec(
            task_id=task_id,
            func_ref=fn.__name__,
            deps=[],
            dynamic_spawns=branch_map,
            constraints=constraints
        )
        return task_id

    def map_reduce(self, mapper: Callable, reducer: Callable, count: int) -> str:
        """
        Create map-reduce pattern: N identical mapper tasks → 1 reducer task

        Args:
            mapper: Function executed count times in parallel
            reducer: Function executed after all mappers complete
            count: Number of mapper instances

        Returns:
            task_id of reducer (for linking downstream tasks)

        Example:
            def mapper():
                print("MAPPING")

            def reducer():
                print("REDUCING")

            reducer_id = wf.map_reduce(mapper, reducer, count=5)
            # Creates 5 parallel mapper tasks, then 1 reducer
        """
        # Create count mapper tasks
        mapper_ids = []
        for i in range(count):
            mapper_id = self.task(mapper)
            mapper_ids.append(mapper_id)

        # Create reducer task
        reducer_id = self.task(reducer)

        # Link all mappers → reducer
        for mapper_id in mapper_ids:
            self.link(mapper_id, reducer_id)

        return reducer_id
