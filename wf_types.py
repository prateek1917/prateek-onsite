from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from enum import Enum
import uuid

class TaskType(str, Enum):
    DEFAULT = "DEFAULT"

@dataclass
class TaskSpec:
    task_id: str # UUID
    name: str
    type: TaskType
    func_ref: str
    params: Dict[str, Any] = field(default_factory=dict)
    deps: List[str] = field(default_factory=list)

@dataclass
class TaskResult:
    task_id: str
    state: str
    output: Any = None
    error: Optional[str] = None
