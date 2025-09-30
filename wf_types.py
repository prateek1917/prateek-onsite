from dataclasses import dataclass, field
from typing import List

@dataclass
class TaskSpec:
    task_id: str        # UUID
    func_ref: str       # Function name in registry
    deps: List[str] = field(default_factory=list)  # List of task_ids this depends on
