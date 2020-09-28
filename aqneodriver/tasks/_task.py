import functools
from abc import ABCMeta
from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Event
from threading import Thread
from typing import Callable
from typing import Optional
from typing import Tuple
from typing import TypeVar

from omegaconf import DictConfig
from omegaconf import OmegaConf
from pydent.aqsession import AqSession
from rich import inspect
from rich import print
from rich.panel import Panel

from aqneodriver._version import __title__
from aqneodriver.driver import AquariumETLDriver
from aqneodriver.exceptions import HelpException
from aqneodriver.loggers import logger

T = TypeVar("T")


def print_task_header(taskname):
    print(
        Panel(
            "Running task [blue]" + taskname + "[/blue]",
            title="[green]" + __title__ + "[/green]",
        )
    )


def print_help(d):
    print(d)


@contextmanager
def log_level_context(log_level: str):
    prev_level = logger.getEffectiveLevel()
    logger.setLevel(log_level)
    yield
    logger.setLevel(prev_level)


class RegisteredTask(ABCMeta):
    """A registered task.

    All tasks have a unique `name` indicating the action to take. Each
    class must implement a `run(self, cfg: DictConfig)` method that
    performs the task using config parameters. Immediate task parameters
    are stored in `self` for the given task.
    """

    registered_tasks = {}
    ignore_classes = {"Task"}

    def __new__(cls, clsname, superclasses, attributedict):
        # wrap `run` so that it logs when the task starts & completes
        if clsname not in cls.ignore_classes:
            attributedict["run"] = cls._log_run_wrapper(
                cls._help_run_wrapper(attributedict["run"])
            )

        # create the new class
        newcls = super().__new__(cls, clsname, superclasses, attributedict)

        # register the class with unique name
        if clsname not in cls.ignore_classes:
            name = attributedict["name"]
            if name in RegisteredTask.registered_tasks:
                raise ValueError("task name {} already registered".format(name))
            RegisteredTask.registered_tasks[name] = newcls
        return newcls

    @staticmethod
    def _log_run_wrapper(
        f: Callable[[Optional[DictConfig]], T]
    ) -> Callable[[Optional[DictConfig]], T]:
        @functools.wraps(f)
        def wrapped(self, cfg, **kwargs):
            with log_level_context(self.log_level):
                logger.info("running {}".format(str(self)))
                result = f(self, cfg, **kwargs)
                logger.info("completed {}".format(str(self)))
            return result

        return wrapped

    @staticmethod
    def _help_run_wrapper(
        f: Callable[[Optional[DictConfig]], T]
    ) -> Callable[[Optional[DictConfig]], T]:
        @functools.wraps(f)
        def wrapped(self, cfg, **kwargs):
            if cfg.help is True:
                self.help()
            else:
                print_task_header(self.name)
                return f(self, cfg, **kwargs)

        return wrapped

    def get_task(cls, cfg: DictConfig):
        task_cls = cls.registered_tasks.get(cfg.task.name)
        task_inst = task_cls(**cfg.task)
        return task_inst

    def run_task(cls, cfg: DictConfig):
        return cls.get_task(cfg).run(cfg)


# TODO: implement dryrun for driver
@dataclass
class Task(metaclass=RegisteredTask):
    """A registered task."""

    name: str = "GenericTask"  #: The task name. Must be unique for each task.
    log_level: str = "ERROR"  #: Sets the log level for the task. Will return to prior log level on completion.
    timeout: int = 10

    @staticmethod
    def get_driver(cfg: DictConfig) -> AquariumETLDriver:
        """Get the :class:`AquariumETLDriver.

        <aqneodriver.driver.AquariumETLDriver>` from configuration file.

        :param cfg: The configuration file
        :return: The :class:`AquariumETLDriver <aqneodriver.driver.AquariumETLDriver>`
        """
        logger.info("Initializing Neo4j Driver...")
        return AquariumETLDriver(cfg.neo.uri, cfg.neo.user, cfg.neo.password)

    @staticmethod
    def get_aq(cfg: DictConfig) -> AqSession:
        """Get the :class:`AqSession <pydent.aqsession.AqSession>` from
        configuration file.

        :param cfg: The configuration file
        :return: The :class:`AqSession <pydent.aqsession.AqSession>`
        """
        logger.info("Initializing Aquarium Driver...")
        return AqSession(cfg.aquarium.user, cfg.aquarium.password, cfg.aquarium.uri)

    def sessions(self, cfg: DictConfig) -> Tuple[AquariumETLDriver, AqSession]:
        """Return sessions with a timeout applied.

        :param cfg: The configuration file
        :return: Tuple of neo driver and AqSession
        """
        event = Event()

        def aqlogin():
            event.aqsession = self.get_aq(cfg)

        def neologin():
            event.neosession = self.get_driver(cfg)

        aq_thread = Thread(target=aqlogin)
        neo_thread = Thread(target=neologin)
        aq_thread.start()
        neo_thread.start()

        aq_thread.join(timeout=cfg.task.timeout)
        neo_thread.join(timeout=cfg.task.timeout)
        return event.neosession, event.aqsession

    def help(self):
        inspect(self, help=True, title="{} help".format(self.name))
        raise HelpException

    @abstractmethod
    def run(self, cfg: DictConfig):
        """Run the task. Must be implemented for each task.

        .. warning::

            It is highly recommended to primarily use the :py:`cfg` argument to access
            configuration values as these will be type checked. However,
            instance variables (not checked) are still available via
            :py:`self`
        """
        print_task_header(self.name)

    def __call__(self, cfg: DictConfig):
        """Run an task instance by merging the task's instance variables with
        the configuration.

        .. note::

            This will update the current configuration is the
            instance of this task. These variables can then
            be accessed in the :meth:`~aqneoetl.tasks.Task.run`

        :param cfg: the configuration that will be merged
        :return: task result
        """
        # TODO: unsure if this is the right way to update the config
        #       Currently, setting this will raise an error
        self._merge(cfg)
        return self.run(cfg)

    def _merge(self, cfg: DictConfig):
        OmegaConf.set_struct(cfg, False)
        OmegaConf.update(cfg, "task", self, merge=True)
        OmegaConf.set_struct(cfg, True)
        return cfg

    def __str__(self):
        return "<{}(Task) id={}>".format(self.__class__.__name__, id(self))

    def __repr__(self):
        return self.__str__()
