import uuid
from typing import Dict, Callable, List, Optional, Set
from wf_types import TaskSpec, Constraint, StaticConstraint, NoOutgoingEdgesConstraint, NoIncomingEdgesConstraint
from func_registry import register, has

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
            # Static task - wrap to hide ctx
            task_id = str(uuid.uuid4())

            # Only register wrapper if not already registered
            if not has(fn.__name__):
                user_fn = fn
                def wrapper(_):  # _ == ctx which is unused in static
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
        Create map-reduce pattern: mapper_initiator → N mappers → 1 reducer

        Args:
            mapper: Function executed count times in parallel
            reducer: Function executed after all mappers complete
            count: Number of mapper instances

        Returns:
            task_id of mapper_initiator (entry point for linking upstream tasks)

        Example:
            def mapper():
                print("MAPPING")

            def reducer():
                print("REDUCING")

            mr_id = wf.map_reduce(mapper, reducer, count=5)
            # Creates: mapper_initiator → 5 parallel mappers → 1 reducer
            wf.link(upstream_task, mr_id)  # Links to mapper_initiator
        """
        # Create mapper initiator (no-op entry point)
        mapper_initiator_id = self.task(lambda: None)

        # Create count mapper tasks
        mapper_ids = []
        for i in range(count):
            mapper_id = self.task(mapper)
            mapper_ids.append(mapper_id)
            # Link mapper_initiator → mapper
            self.link(mapper_initiator_id, mapper_id)

        # Create reducer task
        reducer_id = self.task(reducer)

        # Link all mappers → reducer
        for mapper_id in mapper_ids:
            self.link(mapper_id, reducer_id)

        return mapper_initiator_id

    def visualize(self, filename: str = "workflow", view: bool = False):
        """
        Generate a graphviz visualization of the workflow DAG.

        Args:
            filename: Output filename (without extension)
            view: If True, open the generated image automatically

        Returns:
            graphviz.Digraph object

        Notes:
            - Solid edges: static dependencies
            - Dotted edges: dynamic branches (conditionally spawned at runtime)
            - Tasks in dynamic_spawns are marked with "(dynamic)" label
        """
        try:
            import graphviz
        except ImportError:
            raise ImportError("graphviz package required. Install with: pip install graphviz")

        # Collect all dynamically spawned task IDs
        dynamic_task_ids: Set[str] = set()
        for task in self._tasks.values():
            if task.dynamic_spawns:
                dynamic_task_ids.update(task.dynamic_spawns.values())

        # Create directed graph
        dot = graphviz.Digraph(comment=self.name)
        dot.attr(rankdir='TB')  # Top to bottom layout

        # Add nodes
        for task_id, task in self._tasks.items():
            label = task.func_ref
            if task_id in dynamic_task_ids:
                label += "\n(dynamic)"

            # Style dynamic tasks differently
            if task_id in dynamic_task_ids:
                dot.node(task_id, label, shape='box', style='rounded,dashed', color='blue')
            elif task.dynamic_spawns:
                # Task that spawns dynamic branches
                dot.node(task_id, label, shape='diamond', style='filled', fillcolor='lightblue')
            else:
                dot.node(task_id, label, shape='box', style='rounded')

        # Add edges
        for task_id, task in self._tasks.items():
            # 1. Draw solid edges for regular dependencies (compile-time)
            for dep_id in task.deps:
                dot.edge(dep_id, task_id)

            # 2. Draw dotted edges for dynamic spawns (runtime-conditional)
            if task.dynamic_spawns:
                for spawn_label, spawn_id in task.dynamic_spawns.items():
                    dot.edge(task_id, spawn_id, style='dotted', color='blue',
                            label=spawn_label)

        # Render to file
        dot.render(filename, format='png', cleanup=True, view=view)

        return dot 
