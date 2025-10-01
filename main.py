from client.workflow import Workflow
from server.orchestrator import Orchestrator
import random

wf = Workflow("branching_demo")

def load():
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

def process_high():
    print("PROCESSING HIGH VALUE")

def process_low():
    print("PROCESSING LOW VALUE")

# Build workflow
t_load = wf.task(load)
t_load_2 = wf.task(load)

# Example 1: Branching task - returns exactly ONE branch
print("=== Example 1: Branching Task (exactly one) ===")
t_eval_one = wf.branched_task(evaluate_high_or_low, [process_high, process_low])
t_high_1 = wf.get_task(process_high)
t_low_1 = wf.get_task(process_low)

wf.link(t_load, t_eval_one)
wf.link(t_high_1, t_load_2)
wf.link(t_low_1, t_load_2)

result = Orchestrator().run(wf)
print("\nWorkflow execution result:", result)

# Example 2: Dynamic task - can return multiple branches
print("\n\n=== Example 2: Dynamic Task (can return multiple) ===")
wf2 = Workflow("dynamic_demo")
t_load2 = wf2.task(load)
t_load_2_2 = wf2.task(load)

t_eval_multi = wf2.task(evaluate_low_maybe_high, possible_branches=[process_high, process_low])
t_high_2 = wf2.get_task(process_high)
t_low_2 = wf2.get_task(process_low)

wf2.link(t_load2, t_eval_multi)
wf2.link(t_low_2, t_load_2_2)
wf2.link(t_high_2, t_load_2_2)

result2 = Orchestrator().run(wf2)
print("\nWorkflow execution result:", result2)
