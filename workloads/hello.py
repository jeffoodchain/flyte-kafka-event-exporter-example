import flyte

env = flyte.TaskEnvironment(name="hello_env")

@env.task
def fn(x: int) -> int:
    return 2 * x + 5

@env.task
def main(x_list: list[int] = list(range(10))) -> float:
    y_list = list(flyte.map(fn, x_list))
    return sum(y_list) / len(y_list)
