from dataclasses import dataclass
from typing import Optional

from neo4j.exceptions import ConstraintError
from omegaconf import DictConfig
from omegaconf import MISSING
from tqdm.auto import tqdm

from ._task import Task
from aqneodriver.loggers import logger
from aqneodriver.aq_tools import aq_samples_to_cypher


@dataclass
class Query:
    n_samples: int = MISSING
    user: Optional[str] = None


@dataclass
class UpdateSampleDatabase(Task):
    """Populate the Neo4j database with Aquarium samples and their relationships.

    Update the following relationships:

    ::

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
    query: Query = Query()

    @staticmethod
    def catch_constraint_error(e: Exception):
        if not isinstance(e, ConstraintError):
            raise e

    def run(self, cfg: Optional[DictConfig] = None):
        """
        Update the graphdb using some Aquarium sample query.

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

        pbar0 = tqdm(desc="collecting aquarium models")
        node_payload, edge_payload = aq_samples_to_cypher(
            aq, models, new_node_callback=lambda k, d: pbar0.update()
        )

        if cfg.task.strict:
            def error_callback(e: Exception):
                raise e
        else:
            error_callback = self.catch_constraint_error

        pbar1 = tqdm(desc="writing nodes", total=len(node_payload))
        driver.pool(cfg.task.n_jobs).write(
            node_payload,
            callback=lambda _: pbar1.update(),
            chunksize=cfg.task.chunksize,
            error_callback=error_callback,
        )

        pbar2 = tqdm(desc="writing edges", total=len(edge_payload))
        driver.pool(cfg.task.n_jobs).write(
            edge_payload,
            callback=lambda _: pbar2.update(),
            chunksize=cfg.task.chunksize,
            error_callback=error_callback,
        )
