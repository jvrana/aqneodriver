import os
from dataclasses import dataclass
from typing import Optional

from neo4j.exceptions import ConstraintError
from omegaconf import DictConfig
from rich.progress import BarColumn
from rich.progress import DownloadColumn
from rich.progress import Progress
from rich.progress import TimeRemainingColumn
from rich.progress import TransferSpeedColumn

from ._task import Task
from aqneodriver.loggers import logger
from aqneodriver.structured_queries.aquarium import aq_inventory_to_cypher


@dataclass
class InventoryQuery:
    user: Optional[str] = None
    n_items: int = -1


@dataclass
class UpdateInventory(Task):
    """Populates the Neo4j graph db with Aquarium inventory.

    .. note::

        Does not do this in a recursive way, but rather will collect all
        current samples in the graph database, retrieve items, and populate
        the graph db.


    Update the following relationships:

    ::

        (a:Sample) -[hasItem]-> (b:Item)
        (a:Item) -[hasSample]-> (b:Sample)
        (a:Item) -[hasObjectType]-> (b:ObjectType)
        (a:Sample) -[partOf]-> (b:Collection)
        (a:Collection) -[hasSample]-> (b:Sample)

    .. warning::

        This query often takes a **very** long time
    """

    name: str = "update_inventory"  #: the task name
    n_jobs: Optional[int] = None  #: number of parallel jobs to run
    chunksize: int = 100  #: chunksize for each parallel job (default: 100)
    strict: bool = True  #: if False, will catch ConstraintErrors if they arise
    query: InventoryQuery = InventoryQuery()
    create_nodes: bool = True

    @staticmethod
    def catch_constraint_error(e: Exception):
        if not isinstance(e, ConstraintError):
            raise e

    def run(self, cfg: DictConfig):
        if not cfg.task.query:
            raise ValueError("Query must be provided.")
        driver, aq = self.sessions(cfg)
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn(),
            TransferSpeedColumn(),
            DownloadColumn(),
        ) as progress:
            n_cpus = cfg.task.n_jobs or os.cpu_count()

            # TASK 0
            logger.info("Requesting Aquarium inventory...")

            match_query = "MATCH (n:Sample) RETURN n.id"
            if cfg.task.query.n_items > -1:
                match_query += " LIMIT {}".format(cfg.task.query.n_items)
            results = driver.read(match_query)
            sample_ids = [r[0] for r in results]

            models = aq.Sample.find(sample_ids)
            logger.info("Found {} samples in graph db".format(len(models)))

            node_payload = aq_inventory_to_cypher(
                aq,
                models
            )

            if cfg.task.strict:

                def error_callback(e: Exception):
                    raise e

            else:
                error_callback = self.catch_constraint_error

            task1 = progress.add_task("adding nodes...")
            # TODO: indicate when creation is skipped
            if cfg.task.create_nodes:
                progress.tasks[task1].total = len(node_payload)
                driver.pool(n_cpus).write(
                    node_payload,
                    callback=lambda _: progress.update(task1, advance=1),
                    chunksize=cfg.task.chunksize,
                    error_callback=error_callback,
                )
                progress.update(task1, completed=progress.tasks[task1].total)
