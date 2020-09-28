import os
from dataclasses import dataclass
from typing import Optional

from omegaconf import DictConfig
from omegaconf import MISSING
from rich.progress import Progress

from aqneodriver.structured_queries.aquarium import aq_jobs_to_cypher
from aqneodriver.tasks import Task


@dataclass
class JobsQuery:
    n_samples: int = MISSING
    user: Optional[str] = None


@dataclass
class UpdateJobs(Task):
    """Populates the Neo4j graph db with Aquarium inventory.

    .. warning::

        This query often takes a **very** long time
    """

    #
    name: str = "update_jobs"  #: the task name
    query: JobsQuery = JobsQuery()
    n_jobs: Optional[int] = None
    chunksize: int = 100
    strict: bool = True
    create_nodes: bool = True  #: whether to create nodes on the graphdb

    def run(self, cfg: DictConfig):
        driver, aq = self.sessions(cfg)
        sample_ids = driver.read("MATCH (n:Sample) RETURN n.id")
        samples = aq.Sample.where({"id": sample_ids})

        with Progress() as progress:
            n_cpus = cfg.task.n_jobs or os.cpu_count()

            task0 = progress.add_task("writing nodes...")
            payloads = aq_jobs_to_cypher(aq, samples)
            progress.tasks[task0].total = sum([len(x) for x in payloads])

            if cfg.task.strict:

                def error_callback(e: Exception):
                    raise e

            else:
                error_callback = self.catch_constraint_error

            if cfg.task.create_nodes:
                driver.pool(n_cpus).write(
                    payloads,
                    callback=lambda x: progress.update(task0, advance=len(x)),
                    error_callback=error_callback,
                    chunksize=cfg.task.chunksize,
                )
            progress.update(task0, completed=progress.tasks[task0].total)


#
#     @staticmethod
#     def catch_constraint_error(e: Exception):
#         if not isinstance(e, ConstraintError):
#             raise e
#
#     def run(self, cfg: DictConfig):
#         driver, aq = self.sessions(cfg)
#
#         results = driver.read("MATCH (n:Sample) RETURN n.id LIMIT {}".format(cfg.task.query.n_samples))
#         sample_ids = [r[0] for r in results]
#
#         models = aq.Sample.find(sample_ids)
#
#
#         driver, aq = self.sessions(cfg)
#
#         logger.info("Requesting Aquarium inventory...")
#         if not cfg.task.query:
#             raise ValueError("Query must be provided.")
#         query = {}
#         if cfg.task.query.user:
#             user = aq.User.where({"login": cfg.task.query.user})[0]
#             query["user_id"] = user.id
#         driver, aq = self.sessions(cfg)
#
#         match_query = "MATCH (n:Sample) RETURN n.id"
#         if cfg.task.query.n_samples > -1:
#             match_query += " LIMIT {}".format(cfg.task.query.n_samples)
#         results = driver.read(match_query)
#         sample_ids = [r[0] for r in results]
#
#         models = aq.Sample.find(sample_ids)
#         logger.info("Found {} samples in graph db".format(len(models)))
#
#         pbar0 = tqdm(desc="collecting aquarium inventory")
#         node_payload, edge_payload = aq_jobs_to_cypher(
#             aq, models, new_node_callback=lambda k, d: pbar0.update()
#         )
#
#         if cfg.task.strict:
#             def error_callback(e: Exception):
#                 raise e
#         else:
#             error_callback = self.catch_constraint_error
#
#         pbar1 = tqdm(desc="writing nodes", total=len(node_payload))
#         driver.pool(cfg.task.n_jobs).write(
#             node_payload,
#             callback=lambda _: pbar1.update(),
#             chunksize=cfg.task.chunksize,
#             error_callback=error_callback,
#         )
#
#         pbar2 = tqdm(desc="writing edges", total=len(edge_payload))
#         driver.pool(cfg.task.n_jobs).write(
#             edge_payload,
#             callback=lambda _: pbar2.update(),
#             chunksize=cfg.task.chunksize,
#             error_callback=error_callback,
#         )
