import uuid
from enum import Enum
from typing import Callable, Dict, List, Optional, Any
from dataclasses import dataclass, field

# -----------------------------
# Task Types
# -----------------------------
class TaskType(str, Enum):
    SOURCE = "DEFAULT"


# -----------------------------
# Task + decorator
# -----------------------------
@dataclass(eq=False)
class Task:
    name: str
    run_fn: Callable[[dict], Any]
    type: TaskType = TaskType.DEFAULT
    uuid: str = field(default_factory=lambda: str(uuid.uuid4()))
    dependencies: List["Task"] = field(default_factory=list)
    _result: Any = None

    def add_dependency(self, task: "Task"):
        if task is self:
            raise ValueError("A task cannot depend on itself.")
        self.dependencies.append(task)

    def run(self, ctx: dict) -> Any:
        out = self.run_fn(ctx)
        self._result = out
        return out

    def result(self) -> Any:
        return self._result

    def __hash__(self):
        return hash(self.uuid)

    def __repr__(self):
        return f"Task(name={self.name}, type={self.type}, uuid={self.uuid[:8]})"


def task(*, type: TaskType = TaskType.DEFAULT):
    """
    Decorator: turn a function (ctx -> Any) into a typed Task factory.
    Usage:
        @task(type=TaskType.DEFAULT)
        def load(ctx): ...
        t = load()            # -> Task instance
    """
    def wrapper(fn: Callable[[dict], Any]):
        def factory(_name: Optional[str] = None) -> Task:
            return Task(name=fn.__name__, run_fn=fn, type=type)
        factory.__name__ = fn.__name__
        return factory
    return wrapper
