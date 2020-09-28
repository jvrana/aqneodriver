import os
from dataclasses import dataclass
from typing import Optional

from neo4j.exceptions import ConstraintError
from omegaconf import DictConfig
from omegaconf import MISSING
from rich.progress import BarColumn
from rich.progress import DownloadColumn
from rich.progress import Progress
from rich.progress import TimeRemainingColumn
from rich.progress import TransferSpeedColumn

from ._task import Task
from aqneodriver.loggers import logger
from aqneodriver.structured_queries.aquarium import aq_samples_to_cypher


@dataclass
class Query:
    n_samples: int = MISSING
    user: Optional[str] = None


@dataclass
class UpdateSampleDatabase(Task):
    """Populate the Neo4j database with Aquarium samples and their
    relationships, by recursively traversing all sample->field_value->sample
    relationships.

    Update the following relationships:

    .. code-block:: cypher

        (a:Sample) -[hasSampleType]-> (b:SampleType)
        (a:Sample) -[hasFieldValue]-> (b:FieldValue)
        (a:FieldValue) -[hasSample]-> (b:Sample)
        (a:FieldValue) -[hasFieldType]-> (b:FieldType)
    """

    name: str = "update_samples"  #: the task name
    n_jobs: Optional[int] = None  #: number of parallel jobs to run
    chunksize: int = 100  #: chunksize for each parallel job (default: 100)
    strict: bool = True  #: if False, will catch ConstraintErrors if they arise
    on_collision: str = "ignore"  # TODO: implement on_collision
    query: Query = Query()  #: query information for Aquarium/Pydent
    create_nodes: bool = True  #: whether to create nodes on the graphdb

    @staticmethod
    def catch_constraint_error(e: Exception):
        if not isinstance(e, ConstraintError):
            raise e

    def run(self, cfg: Optional[DictConfig] = None):
        """Update the graphdb using some Aquarium sample query.

        :param cfg: the configuration
        :return: None
        """
        driver, aq = self.sessions(cfg)

        logger.info("Requesting Aquarium inventory...")
        if not cfg.task.query:
            raise ValueError("Query must be provided.")
        query = {}
        if cfg.task.query.user:
            user = aq.User.where({"login": cfg.task.query.user})[0]
            query["user_id"] = user.id
        n_samples = cfg.task.query.n_samples
        models = aq.Sample.last(n_samples, query)

        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn(),
            TransferSpeedColumn(),
            DownloadColumn(),
        ) as progress:
            n_cpus = cfg.task.n_jobs or os.cpu_count()
            task0 = progress.add_task(
                "[blue]collecting Aquarium samples[/blue]", total=10000
            )

            if cfg.task.strict:

                def error_callback(e: Exception):
                    raise e

            else:
                error_callback = self.catch_constraint_error

            if cfg.task.create_nodes:
                task1 = progress.add_task(
                    "[red]writing edges to [bold]neo4j[/bold]...[/red] ([green]cpus: {cpus}[/green])".format(
                        cpus=n_cpus
                    )
                )
            else:
                task1 = None

            node_payload = aq_samples_to_cypher(
                aq,
                models,
                new_node_callback=lambda k, d: progress.update(task0, advance=1),
            )

            if cfg.task.create_nodes:
                progress.tasks[task1].total = len(node_payload)
                driver.pool(n_cpus).write(
                    node_payload,
                    callback=lambda x: progress.update(task1, advance=len(x)),
                    chunksize=cfg.task.chunksize,
                    error_callback=error_callback,
                )
                progress.update(task1, completed=progress.tasks[task1].total)

            # if cfg.task.strict:
            #
            #     def error_callback(e: Exception):
            #         raise e
            # else:
            #     error_callback = self.catch_constraint_error
            #
            # # TODO: indicate when creation is skipped
            # if cfg.task.create_nodes:
            #     progress.tasks[task1].total = len(node_payload)
            #     driver.pool(n_cpus).write(
            #         node_payload,
            #         callback=lambda _: progress.update(task1, advance=1),
            #         chunksize=cfg.task.chunksize,
            #         error_callback=error_callback,
            #     )
            #     progress.update(task1, completed=progress.tasks[task1].total)
            #
            # if cfg.task.create_edges:
            #     progress.tasks[task2].total = len(edge_payload)
            #     driver.pool(n_cpus).write(
            #         edge_payload,
            #         callback=lambda _: progress.update(task2, advance=1),
            #         chunksize=cfg.task.chunksize,
            #         error_callback=error_callback,
            #     )
            #     progress.update(task1, completed=progress.tasks[task2].total)
