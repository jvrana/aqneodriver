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
class UpdateDatabase(Task):
    """Update the Neo4j database."""

    name: str = "update_db"  #: the task name
    n_jobs: int = MISSING  #: number of parallel jobs to run
    chunksize: int = 100  #: chunksize for each parallel job (default: 100)
    strict: bool = True  #: if False, will catch ConstraintErrors if they arise
    query: str = MISSING

    @staticmethod
    def catch_constraint_error(e: Exception):
        if not isinstance(e, ConstraintError):
            raise e

    def run(self, cfg: Optional[DictConfig] = None):
        # if cfg.task.query:
        print(cfg.task.query)
        raise Exception
        driver = self.get_driver(cfg)
        aq = self.get_aq(cfg)

        logger.info("Requesting Aquarium inventory...")
        models = aq.Sample.last(10)

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
