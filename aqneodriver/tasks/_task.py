import functools
from abc import ABCMeta
from abc import abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable
from typing import Optional
from typing import TypeVar

from omegaconf import DictConfig
from omegaconf import OmegaConf
from pydent.aqsession import AqSession

from aqneodriver.driver import AquariumETLDriver
from aqneodriver.loggers import logger

T = TypeVar("T")


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
            attributedict["run"] = cls._log_run_wrapper(attributedict["run"])

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

    def get_task(cls, cfg: DictConfig):
        print(cfg)
        task_cls = cls.registered_tasks.get(cfg.task.name)
        task_inst = task_cls(**cfg.task)
        return task_inst

    def run_task(cls, cfg: DictConfig):
        return cls.get_task(cfg).run(cfg)


# TODO: implement dryrun for driver
@dataclass
class Task(metaclass=RegisteredTask):
    """A registered task."""

    name: str = "GenericTask"
    log_level: str = "ERROR"

    @staticmethod
    def get_driver(cfg: DictConfig) -> AquariumETLDriver:
        logger.info("Initializing Neo4j Driver...")
        return AquariumETLDriver(cfg.neo.uri, cfg.neo.user, cfg.neo.password)

    @staticmethod
    def get_aq(cfg: DictConfig) -> AqSession:
        logger.info("Initializing Aquarium Driver...")
        return AqSession(cfg.aquarium.user, cfg.aquarium.password, cfg.aquarium.host)

    @abstractmethod
    def run(self, cfg: Optional[DictConfig] = None):
        pass

    def __call__(self, cfg: DictConfig):
        subcfg = OmegaConf.structured(self)
        print(subcfg)
        cfg.merge_with(subcfg)
        print(cfg)
        return self.run(cfg)

    def __str__(self):
        return "<{}(Task) id={}>".format(self.__class__.__name__, id(self))

    def __repr__(self):
        return self.__str__()
