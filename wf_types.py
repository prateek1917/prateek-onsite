from dataclasses import dataclass, field
from typing import List, Optional, Dict
from enum import Enum

class Constraint(Enum):
    STATIC = "STATIC"

@dataclass
class TaskSpec:
    task_id: str        # UUID
    func_ref: str       # Function name in registry
    deps: List[str] = field(default_factory=list)  # List of task_ids this depends on
    dynamic_spawns: Optional[Dict[str, str]] = None  # None = static, {"label": task_id} = branching task
    constraints: List[Constraint] = field(default_factory=list)  # Constraints to validate at runtime
