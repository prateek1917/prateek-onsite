from client.workflow import Workflow
from server.orchestrator import Orchestrator

wf = Workflow("test")

def evaluate_multiple():
    """Returns multiple branches - should fail for branched_task"""
    return [process_high, process_low]

def process_high():
    print("HIGH")

def process_low():
    print("LOW")

# This should fail at runtime
t_eval = wf.branched_task(evaluate_multiple, [process_high, process_low])

try:
    Orchestrator().run(wf)
    print("ERROR: Should have raised RuntimeError!")
except RuntimeError as e:
    print(f"✓ Correctly raised error: {e}")
