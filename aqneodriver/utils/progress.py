from contextlib import contextmanager
from rich.progress import Progress


def update_infinite_task(progress: Progress, task_id: int, n: float = 1.5):
    def _update(idx: int, results: list):
        progress.update(task_id, advance=len(results))
        progress.refresh()
        task = progress.tasks[task_id]
        if task.completed >= task.total:
            progress.update(task_id, completed=0)
            task.total = task.total * n
    return _update


def update_task(progress: Progress, task_id: int):
    def _update(a, b):
        progress.update(task_id, advance=len(b))
        progress.refresh()
    return _update


def finish_task(progress: Progress, task_id):
    progress.update(task_id, completed=progress.tasks[task_id].total)
    progress.refresh()


@contextmanager
def task_context(progress, task_id):
    yield update_task(progress, task_id)
    finish_task(progress, task_id)


@contextmanager
def infinite_task_context(progress, task_id, n=1.25):
    yield update_infinite_task(progress, task_id, n=n)
    finish_task(progress, task_id)