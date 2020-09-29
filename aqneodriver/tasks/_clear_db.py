from dataclasses import dataclass
from typing import Optional

from omegaconf.dictconfig import DictConfig
from rich import print
from rich.panel import Panel

from ._task import Task
from aqneodriver.exceptions import HelpException
from aqneodriver.loggers import logger


@dataclass
class ClearDatabase(Task):
    """Clear the Neo4j database."""

    name: str = "clear_db"  #: the task name
    force: bool = False
    only_edges: bool = False


    def run(self, cfg: Optional[DictConfig]):
        """Clear the graph db.

        .. warning::

            **task.force = true** must be passed, else :class:`RuntimeError` will be raised.

        :param cfg: the configuration file
        :return: None
        """
        driver = self.get_driver(cfg)
        logger.info("clearing database...")
        if not cfg.task.force:
            print(
                Panel(
                    "This task will delete the entire database\n"
                    "Please use `task.force=true` to force db clearing.",
                    style="white on red",
                    title="warning",
                    expand=False,
                )
            )
            self.help()
            raise HelpException(
                "This task will delete the entire database"
                " Please use task.force=true to force db clearing."
            )
        else:
            if cfg.task.only_edges:
                driver.clear_relationships()
            else:
                driver.clear()
