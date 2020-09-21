import logging
import os
from typing import List
from typing import TypeVar

from hydra.experimental import compose
from hydra.experimental import initialize_config_dir
from omegaconf import DictConfig
from py.path import local
from pydent.utils import logger as pydent_logger
from .configstore import init_config_store


pydent_logger.set_level("ERROR")
logger = logging.getLogger(__file__)
logging.getLogger().setLevel(logging.INFO)

T = TypeVar("T")


def get_config(
    overrides: List[str] = None,
    config_path: str = "conf",
    config_name: str = "config",
    directory: str = None,
) -> DictConfig:
    init_config_store()
    directory = directory or os.getcwd()
    with local(directory).as_cwd():
        overrides = overrides or []
        config_path = os.path.join(directory, config_path)
        with initialize_config_dir(config_path):
            cfg = compose(config_name=config_name, overrides=overrides)

        # # correct port from docker-compose file
        # ports = cfg.docker.services.neo4j.ports
        # port_mapping = {}
        # for p in ports:
        #     _from, _to = p.split(":")
        #     port_mapping[_from] = _to
        # cfg.neo.port = port_mapping["7687"]
        return cfg
