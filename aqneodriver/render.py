import inspect
from rich.panel import Panel
from rich.console import RenderGroup
from rich import print
from aqneodriver.tasks import Task


def render_on(c=None, *, auto=True):
    def wrapped(f):
        if c is None and auto is True:
            argspec = inspect.getfullargspec(f)
            klass = argspec.annotations[argspec.args[0]]
        else:
            klass = c
        f.__render_on__ = klass
        return f

    return wrapped


def render(c):
    c.__render_methods__ = {}
    for k in dir(c):
        if not k.startswith('__'):
            v = getattr(c, k)
            if hasattr(v, '__render_on__'):
                c.__render_methods__[v.__render_on__] = v
    return c


@render
class Renderer(object):
    __render_methods__ = {}

    @staticmethod
    @render_on()
    def render_task_help(task: Task):
        print(
            Panel(
                RenderGroup(
                    Panel("Task: {}".format(task.name)),
                    Panel(task.__doc__)
                )
            )
        )

    @staticmethod
    def render(obj):
        f = Renderer.__render_methods__.get(obj.__class__, None)
        if not f:
            for k, _f in Renderer.__render_methods__.items():
                if issubclass(obj.__class__, k):
                    f = _f
                    break
        if f is not None:
            return f(obj)
        raise ValueError


class Help(object):

    def help(self):
        Renderer.render(self)