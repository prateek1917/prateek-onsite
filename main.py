from workflow import Workflow
from task import task, TaskType

@task(type=TaskType.DEFAULT)
def load_numbers(ctx):
    # pretend read from somewhere
    data = [1, 2, 3, 4]
    ctx["numbers"] = data
    return data

@task(type=TaskType.DEFAULT)
def sum_numbers(ctx):
    nums = ctx.get("numbers", [])
    return sum(nums)

@task(type=TaskType.DEFAULT)
def double_sum(ctx):
    return ctx.get("sum_numbers", 0) * 2  # we stored results by name in ctx

@task(type=TaskType.DEFAULT)
def write_report(ctx):
    # just print; later you can enforce “no outgoing edges” for SINKs
    print("Report:", {"numbers": ctx.get("numbers"), "sum": ctx.get("sum_numbers"), "double": ctx.get("double_sum")})
    return "ok"
    
wf = Workflow("typed-dag")

t_load   = wf.add_task(load_numbers())
t_sum    = wf.add_task(sum_numbers())
t_double = wf.add_task(double_sum())  # explicit name for ctx lookup
t_write  = wf.add_task(write_report())

# Wire edges
wf.static_link_tasks(t_load, t_sum)     # load → sum
wf.static_link_tasks(t_sum, t_double)   # sum → double
wf.static_link_tasks(t_double, t_write) # double → sink

# Run
ctx = wf.run()
print("Final ctx keys:", list(ctx.keys()))