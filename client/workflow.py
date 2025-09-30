import uuid
from typing import Dict, Callable
from wf_types import TaskSpec
from func_registry import register

class Workflow:
    def __init__(self, name: str = "Workflow"):
        self.name = name
        self._tasks: Dict[str, TaskSpec] = {}

    def task(self, fn: Callable) -> str:
        """Register function, return task_id"""
        task_id = str(uuid.uuid4())
        register(fn.__name__, fn)
        self._tasks[task_id] = TaskSpec(
            task_id=task_id,
            func_ref=fn.__name__,
            deps=[]
        )
        return task_id

    def link(self, upstream_id: str, downstream_id: str):
        """Declare: downstream depends on upstream"""
        self._tasks[downstream_id].deps.append(upstream_id)
