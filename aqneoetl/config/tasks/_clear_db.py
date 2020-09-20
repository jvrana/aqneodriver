from dataclasses import dataclass
from typing import Optional

from omegaconf.dictconfig import DictConfig

from ._task import Task
from aqneoetl.loggers import logger


@dataclass
class ClearDatabase(Task):
    """Clear the Neo4j database."""

    name = "clear_db"  #: the task name

    def run(self, cfg: Optional[DictConfig]):
        driver = self.get_driver(cfg)
        logger.info("clearing database...")
        driver.clear()
