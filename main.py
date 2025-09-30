from client.workflow import Workflow, TaskType
from server.orchestrator import Orchestrator

wf = Workflow("demo")

@wf.task(type=TaskType.DEFAULT)   # ref = "load"
def load(ctx):
    print("LOADING DATA")

@wf.task()                        # ref = "total"
def total(ctx):
    print("GETTING TOTAL")

t_load = load(name="load")
t_sum  = total(name="total")
wf.link(t_load, t_sum)

ctx = Orchestrator().run(wf)
print("sum:", ctx["by_name"]["total"][0])  # 60
