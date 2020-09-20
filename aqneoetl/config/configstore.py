from abc import ABCMeta
from dataclasses import dataclass
from typing import Optional
from typing import TypeVar

from hydra.core.config_store import ConfigStore
from omegaconf import DictConfig
from omegaconf import MISSING
from pydent import AqSession
from tqdm import tqdm

from aqneoetl import AquariumETLDriver
from aqneoetl.loggers import logger
from aqneoetl.queries import aq_to_cypher

T = TypeVar("T")


@dataclass
class Connection:
    host: str = MISSING
    port: int = MISSING
    user: str = MISSING
    password: str = MISSING
    uri: str = "${neo.host}:${neo.port}"


@dataclass
class AquariumConnection(Connection):
    host: str = "http://0.0.0.0"
    port: int = 80
    user: str = "neptune"
    password: str = "aquarium"


@dataclass
class NeoConnetion(Connection):
    host: str = "bolt://localhost"
    port: int = 7687
    user: str = "neo4j"
    password: str = "neo4j"


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

    def get_task(cls, cfg: DictConfig):
        task_cls = cls.registered_tasks.get(cfg.task.name)
        task_inst = task_cls(**cfg.task)
        return task_inst

    def run_task(cls, cfg: DictConfig):
        return cls.get_task(cfg).run(cfg)

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


@dataclass
class Task(metaclass=RegisteredTask):
    name: str = MISSING
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

    def __str__(self):
        return "<{}(Task) id={}>".format(self.__class__.__name__, id(self))

    def __repr__(self):
        return self.__str__()


@dataclass
class UpdateDatabase(Task):
    """Update the Neo4j database."""

    name: str = "update_db"  #: the task name
    n_jobs: int = MISSING  #: number of parallel jobs to run
    chunksize: int = 100  #: chunksize for each parallel job (default: 100)

    def run(self, cfg: Optional[DictConfig] = None):
        driver = self.get_driver(cfg)
        aq = self.get_aq(cfg)

        logger.info("Requesting Aquarium inventory...")
        models = aq.Sample.last(10)

        pbar0 = tqdm(desc="collecting aquarium models")
        node_payload, edge_payload = aq_to_cypher(
            aq, models, new_node_callback=lambda k, d: pbar0.update()
        )

        pbar1 = tqdm(desc="writing nodes", total=len(node_payload))
        driver.pool(self.n_jobs).write(
            node_payload, callback=lambda _: pbar1.update(), chunksize=self.chunksize
        )

        pbar2 = tqdm(desc="writing edges", total=len(edge_payload))
        driver.pool(self.n_jobs).write(
            edge_payload, callback=lambda _: pbar2.update(), chunksize=self.chunksize
        )


@dataclass
class ClearDatabase(Task):
    """Clear the Neo4j database."""

    name = "clear_db"  #: the task name

    def run(self, cfg: Optional[DictConfig]):
        driver = self.get_driver(cfg)
        logger.info("clearing database...")
        driver.clear()


@dataclass
class Config:
    task: Task = MISSING
    aquarium: AquariumConnection = MISSING
    neo: NeoConnetion = MISSING


cs = ConfigStore.instance()
cs.store(name="config", node=Config)
cs.store(group="aquarium", name="default", node=AquariumConnection)
cs.store(group="neo", name="default", node=NeoConnetion)
# cs.store(group='job', name='default', node=Job)

for name, task in Task.registered_tasks.items():
    cs.store(group="task", name=name, node=task)
    cs.store(group="task", name=name, node=task)
