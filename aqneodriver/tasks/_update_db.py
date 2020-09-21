from dataclasses import dataclass
from typing import Optional

from neo4j.exceptions import ConstraintError
from omegaconf import DictConfig
from omegaconf import MISSING
from tqdm import tqdm

from ._task import Task
from aqneodriver.loggers import logger
from aqneodriver.queries.sample_relationships import aq_to_cypher

@dataclass
class Query:
    n_samples: int = MISSING
    user: Optional[str] = None


@dataclass
class UpdateDatabase(Task):
    """Update the Neo4j database."""

    name: str = "update_db"  #: the task name
    n_jobs: Optional[int] = None #: number of parallel jobs to run
    chunksize: int = 100  #: chunksize for each parallel job (default: 100)
    strict: bool = True  #: if False, will catch ConstraintErrors if they arise
    query: Query = Query()

    @staticmethod
    def catch_constraint_error(e: Exception):
        if not isinstance(e, ConstraintError):
            raise e

    def run(self, cfg: Optional[DictConfig] = None):

        driver = self.get_driver(cfg)
        aq = self.get_aq(cfg)

        logger.info("Requesting Aquarium inventory...")
        query = {}
        if self.query.user:
            user = aq.User.where({'login': self.query.user})[0]
            query['user_id'] = user.id
        n_samples = self.query.n_samples
        models = aq.Sample.last(n_samples, query)

        pbar0 = tqdm(desc="collecting aquarium models")
        node_payload, edge_payload = aq_to_cypher(
            aq, models, new_node_callback=lambda k, d: pbar0.update()
        )

        if self.strict:

            def error_callback(e: Exception):
                raise e

        else:
            error_callback = self.catch_constraint_error

        pbar1 = tqdm(desc="writing nodes", total=len(node_payload))
        driver.pool(self.n_jobs).write(
            node_payload,
            callback=lambda _: pbar1.update(),
            chunksize=self.chunksize,
            error_callback=error_callback,
        )

        pbar2 = tqdm(desc="writing edges", total=len(edge_payload))
        driver.pool(self.n_jobs).write(
            edge_payload,
            callback=lambda _: pbar2.update(),
            chunksize=self.chunksize,
            error_callback=error_callback,
        )
