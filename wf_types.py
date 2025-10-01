from dataclasses import dataclass, field
from typing import List, Optional, Dict


class Constraint:
    """Base class for workflow constraints"""
    pass


class StaticConstraint(Constraint):
    """Task cannot be a dynamic/branching task"""
    def __repr__(self):
        return "STATIC"


class NoOutgoingEdgesConstraint(Constraint):
    """No edges can be created starting at this task (task cannot have successors)"""
    def __repr__(self):
        return "NO_OUTGOING_EDGES"


class NoIncomingEdgesConstraint(Constraint):
    """No edges can be created ending at this task (task cannot have dependencies)"""
    def __repr__(self):
        return "NO_INCOMING_EDGES"


@dataclass
class TaskSpec:
    task_id: str        # UUID
    func_ref: str       # Function name in registry
    deps: List[str] = field(default_factory=list)  # List of task_ids this depends on
    dynamic_spawns: Optional[Dict[str, str]] = None  # None = static, {"label": task_id} = branching task
    constraints: List[Constraint] = field(default_factory=list)  # Constraints to validate at runtime
