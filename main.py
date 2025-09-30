from client.workflow import Workflow
from server.orchestrator import Orchestrator
import random

wf = Workflow("branching_demo")

def load(ctx):
    print("LOADING DATA")

def evaluate(ctx):
    """Branching task that decides which path to take"""
    value = random.randint(1, 20)
    print(f"EVALUATING: value = {value}")

    if value > 10:
        print("  -> Taking HIGH branch")
        ctx.spawn_branch("high")
    else:
        print("  -> Taking LOW branch")
        ctx.spawn_branch("low")

def process_high(ctx):
    print("PROCESSING HIGH VALUE")

def process_low(ctx):
    print("PROCESSING LOW VALUE")

# Build workflow
t_load = wf.task(load)
t_load_2 = wf.task(load)
t_eval = wf.task(evaluate)
t_high = wf.task(process_high)
t_low = wf.task(process_low)

# Static dependencies
wf.link(t_load, t_eval)
wf.link(t_low, t_load_2)

# Dynamic branching - eval can spawn either high or low at runtime
wf.branching(t_eval, {"high": t_high, "low": t_low})

print("=== Running Branching Workflow ===")
result = Orchestrator().run(wf)
print("\nWorkflow execution result:", result)
