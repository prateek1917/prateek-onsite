from client.workflow import Workflow
from server.orchestrator import Orchestrator
from wf_types import StaticConstraint, NoOutgoingEdgesConstraint, NoIncomingEdgesConstraint

# Test 1: STATIC constraint on dynamic task should FAIL
print("=== Test 1: STATIC constraint on dynamic task ===")
wf = Workflow("test_static_fail")

def evaluate():
    return process_high

def process_high():
    print("HIGH")

def process_low():
    print("LOW")

t_eval = wf.task(evaluate, possible_branches=[process_high, process_low], constraints=[StaticConstraint()])

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

t_load = wf2.task(load, constraints=[StaticConstraint()])

result = Orchestrator().run(wf2)
print(f"✓ STATIC constraint works correctly for regular tasks")
print(f"Result: {result}")

# Test 3: NO_OUTGOING_EDGES constraint violation
print("\n=== Test 3: NO_OUTGOING_EDGES constraint ===")
wf3 = Workflow("test_no_outgoing")

def task_a():
    print("TASK A")

def task_b():
    print("TASK B")

t_a = wf3.task(task_a, constraints=[NoOutgoingEdgesConstraint()])
t_b = wf3.task(task_b)
wf3.link(t_a, t_b)  # This creates an outgoing edge from t_a

try:
    Orchestrator().run(wf3)
    print("ERROR: Should have raised RuntimeError!")
except RuntimeError as e:
    print(f"✓ Correctly raised error: {e}")

# Test 4: NO_INCOMING_EDGES constraint violation
print("\n=== Test 4: NO_INCOMING_EDGES constraint ===")
wf4 = Workflow("test_no_incoming")

t_a = wf4.task(task_a)
t_b = wf4.task(task_b, constraints=[NoIncomingEdgesConstraint()])
wf4.link(t_a, t_b)  # This creates an incoming edge to t_b

try:
    Orchestrator().run(wf4)
    print("ERROR: Should have raised RuntimeError!")
except RuntimeError as e:
    print(f"✓ Correctly raised error: {e}")

# Test 5: NO_OUTGOING_EDGES constraint OK (no edges)
print("\n=== Test 5: NO_OUTGOING_EDGES constraint OK ===")
wf5 = Workflow("test_no_outgoing_ok")

t_a = wf5.task(task_a, constraints=[NoOutgoingEdgesConstraint()])

result = Orchestrator().run(wf5)
print(f"✓ NO_OUTGOING_EDGES works when no outgoing edges")
print(f"Result: {result}")

# Test 6: NO_INCOMING_EDGES constraint OK (no edges)
print("\n=== Test 6: NO_INCOMING_EDGES constraint OK ===")
wf6 = Workflow("test_no_incoming_ok")

t_a = wf6.task(task_a, constraints=[NoIncomingEdgesConstraint()])

result = Orchestrator().run(wf6)
print(f"✓ NO_INCOMING_EDGES works when no incoming edges")
print(f"Result: {result}")
