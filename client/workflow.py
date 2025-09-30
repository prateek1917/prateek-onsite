# workflow_client.py
import uuid
from typing import Dict, List, Optional, Any, Callable
from wf_types import TaskType, TaskSpec
from func_registry import register, has

class Workflow:
    def __init__(self, name: str = "Workflow"):
        self.name = name
        self._tasks: Dict[str, TaskSpec] = {}

    def task(self, *, type: TaskType = TaskType.DEFAULT):
        def wrapper(fn: Callable[[dict], Any]):
            key = fn.__name__
            register(key, fn)

            def factory(*, name: Optional[str] = None, **params) -> str:
                instance_name = name or fn.__name__
                return self.add_task(
                    name=instance_name,
                    fn=fn,
                    type=type,
                    params=params,        # ONLY user params meant for the callable
                )

            factory.__name__ = fn.__name__
            return factory
        return wrapper

    # Programmatic add also registers with a simple key
    def add_task(
        self,
        *,
        name: str,
        fn: Callable,
        type: TaskType = TaskType.DEFAULT,
        params: Optional[Dict[str, Any]] = None,
    ) -> str:
        if not has(name):
            register(name, fn)

        tid = str(uuid.uuid4())
        self._tasks[tid] = TaskSpec(
            task_id=tid, name=name, type=type, func_ref=name, params=params or {}, deps=[]
        )
        return tid

    def link(self, first: str, second: str):
        self._tasks[second].deps.append(first)
