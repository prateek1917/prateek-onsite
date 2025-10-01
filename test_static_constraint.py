from client.workflow import Workflow
from server.orchestrator import Orchestrator
from wf_types import Constraint

# Test 1: STATIC constraint on dynamic task should FAIL
print("=== Test 1: STATIC constraint on dynamic task ===")
wf = Workflow("test_static_fail")

def evaluate():
    return process_high

def process_high():
    print("HIGH")

def process_low():
    print("LOW")

t_eval = wf.task(evaluate, possible_branches=[process_high, process_low], constraints=[Constraint.STATIC])

try:
    Orchestrator().run(wf)
    print("ERROR: Should have raised RuntimeError!")
except RuntimeError as e:
    print(f"✓ Correctly raised error: {e}")

# Test 2: STATIC constraint on regular task should be OK
print("\n=== Test 2: STATIC constraint on regular task ===")
wf2 = Workflow("test_static_ok")

def load():
    print("LOADING DATA")

t_load = wf2.task(load, constraints=[Constraint.STATIC])

result = Orchestrator().run(wf2)
print(f"✓ STATIC constraint works correctly for regular tasks")
print(f"Result: {result}")
