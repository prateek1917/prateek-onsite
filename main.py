from client.workflow import Workflow
from server.orchestrator import Orchestrator

wf = Workflow("demo")

def load():
    print("LOADING DATA")

def total():
    print("GETTING TOTAL")

t_load = wf.task(load)
t_sum = wf.task(total)
wf.link(t_load, t_sum)

result = Orchestrator().run(wf)
print("Workflow execution result:", result)
