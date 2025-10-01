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

    results = [process_low2]  # Always spawn low
    if value > 10:
        print("  -> Also taking HIGH branch")
        results.append(process_high2)
    print("  -> Taking LOW branch")
    return results

def process_high():
    print("PROCESSING HIGH VALUE")

def process_low():
    print("PROCESSING LOW VALUE")

def process_high2():
    print("PROCESSING HIGH VALUE 2")

def process_low2():
    print("PROCESSING LOW VALUE 2")

def mapper():
    print("  MAPPER executing")

def reducer():
    print("REDUCER executing")

# Build workflow
t_load = wf.task(load)
t_load_2 = wf.task(lambda : print("LOADING DATA 2"))

t_eval_one = wf.branched_task(evaluate_high_or_low, [process_high, process_low])
t_high_1 = wf.get_task(process_high)
t_low_1 = wf.get_task(process_low)

t_eval_maybe = wf.task(evaluate_low_maybe_high, [process_high2, process_low2])
t_high_2 = wf.get_task(process_high2)
t_low_2 = wf.get_task(process_low2)

wf.link(t_load, t_eval_one)
wf.link(t_high_1, t_load_2)
wf.link(t_low_1, t_eval_maybe)

t_mr = wf.map_reduce(mapper, reducer, count=3)
wf.link(t_high_2, t_mr)

print("\nGenerating workflow visualization...")
wf.visualize(filename="workflow_dag", view=False)
print("Saved to: workflow_dag.png\n")

result = Orchestrator().run(wf)
print("\nWorkflow execution result:", result)