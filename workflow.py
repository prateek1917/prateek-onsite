from task import Task
from typing import List, Dict, Optional
from collections import defaultdict, deque

class Workflow:
    def __init__(self, name=None):
        self.name: str = name or "Workflow"
        self.tasks: Dict[str, Task] = {}  # uuid -> Task
        self.adj_list: Dict[str, List[str]] = defaultdict(list)

    def run(self, initial_ctx: Optional[dict] = None) -> dict:
        self._validate_constraints()
        ctx: dict = initial_ctx or {}
        print(f"=== Run: {self.name} ===")

        for t in self._topological_sort():
            print(f"Running: {t.name} [{t.type}] ({t.uuid[:8]})")
            out = t.run(ctx)
            ctx[f"task:{t.uuid}:result"] = out
            # also store by name (last writer wins) for convenience
            ctx[t.name] = out

        return ctx

    def add_task(self, task: Task) -> Task:
        if task.uuid in self.tasks:
            raise ValueError(f"Task {task.name} ({task.uuid}) already added.")
        self.tasks[task.uuid] = task
        return task

    def static_link_tasks(self, first: Task, second: Task):
        second.add_dependency(first)
        self.adj_list[first].append(second)

    def map_reduce(self, input_data: List, mapper_task: Task, reducer_task: Task):
        pass

    # branch tasks. Like a switch statmenet
    def branch(self, task: Task, optional_tasks: List[Task], condition_list: List[str]):
        pass

    def _validate_constraints(self) -> bool:
        # TODO: impl
        return True

    def _topological_sort(self) -> List[Task]:
        indegree = defaultdict(int)
        graph = defaultdict(list)

        for t in self.tasks.values():
            for dep in t.dependencies:
                graph[dep.uuid].append(t.uuid)
                indegree[t.uuid] += 1

        queue = deque([uid for uid in self.tasks if indegree[uid] == 0])
        order: List[Task] = []

        while queue:
            u = queue.popleft()
            self._validate_constraints()
            order.append(self.tasks[u])
            for v in graph[u]:
                indegree[v] -= 1
                if indegree[v] == 0:
                    queue.append(v)

        if len(order) != len(self.tasks):
            raise RuntimeError("Cycle detected in workflow.")
        return order