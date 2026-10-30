import flyte

env = flyte.TaskEnvironment(name="events_probe")

@env.task
def ok(x: int=1) -> int:
    return x + 1

@env.task
def boom(x:int=1) -> int:
    raise ValueError(f"intentional failure, x = {x}")
