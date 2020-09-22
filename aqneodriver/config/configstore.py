from dataclasses import dataclass
from typing import TypeVar

from hydra.core.config_store import ConfigStore
from omegaconf import MISSING

from aqneodriver.tasks import Task
from aqneodriver.loggers import logger


T = TypeVar("T")


@dataclass
class Connection:
    host: str = MISSING
    port: int = MISSING
    user: str = MISSING
    password: str = MISSING
    uri: str = MISSING


@dataclass
class AquariumConnection(Connection):
    host: str = "http://0.0.0.0"
    port: int = 80
    user: str = "neptune"
    password: str = "aquarium"
    uri: str = "${aquarium.host}:${aquarium.port}"


@dataclass
class NeoConnetion(Connection):
    host: str = "bolt://localhost"
    port: int = 7687
    user: str = "neo4j"
    password: str = "neo4j"
    uri: str = "${neo.host}:${neo.port}"


@dataclass
class Config:
    task: Task = MISSING
    aquarium: AquariumConnection = MISSING
    neo: NeoConnetion = MISSING


def init_config_store():
    cs = ConfigStore.instance()
    cs.store(name="config", node=Config)
    cs.store(group="aquarium", name="default", node=AquariumConnection)
    cs.store(group="neo", name="default", node=NeoConnetion)
    # cs.store(group='job', name='default', node=Job)
    for name, task in Task.registered_tasks.items():
        logger.info("Registering task {} ({})".format(name, task.__name__))
        cs.store(group="task", name=name, node=task)
