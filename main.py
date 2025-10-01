from client.workflow import Workflow
from server.orchestrator import Orchestrator
import random

wf = Workflow("branching_demo")

def load(ctx):
    print("LOADING DATA")

def evaluate_high_or_low():
    """Branching task that decides which path to take (ONE branch)"""
    value = random.randint(1, 20)
    print(f"EVALUATING: value = {value}")

    if value > 10:
        print("  -> Taking HIGH branch")
        return process_high
    else:
        print("  -> Taking LOW branch")
        return process_low

def evaluate_low_maybe_high():
    """Dynamic task that decides which paths to take (ALL returned branches)"""
    value = random.randint(1, 20)
    print(f"EVALUATING: value = {value}")

    results = [process_low]  # Always spawn low
    if value > 10:
        print("  -> Also taking HIGH branch")
        results.append(process_high)
    print("  -> Taking LOW branch")
    return results

def process_high(ctx):
    print("PROCESSING HIGH VALUE")

def process_low(ctx):
    print("PROCESSING LOW VALUE")

# Build workflow
t_load = wf.task(load)
t_load_2 = wf.task(load)

# Dynamic task - possible_branches are auto-registered
t_eval_low_maybe_high = wf.task(evaluate_low_maybe_high, possible_branches=[process_high, process_low])

# Get task IDs for the branches (auto-created by dynamic task)
t_high = wf.get_task(process_high)
t_low = wf.get_task(process_low)

# Static dependencies
wf.link(t_load, t_eval_low_maybe_high)
wf.link(t_low, t_load_2)  # t_load_2 waits for low branch
wf.link(t_high, t_load_2)  # t_load_2 waits for high branch

print("=== Running Branching Workflow ===")
result = Orchestrator().run(wf)
print("\nWorkflow execution result:", result)
