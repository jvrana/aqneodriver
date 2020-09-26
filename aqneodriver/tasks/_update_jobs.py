# flake8: no qa
# from dataclasses import dataclass
# from typing import Optional
#
# from neo4j.exceptions import ConstraintError
#
# from ._task import Task
# from omegaconf import DictConfig, MISSING
# from aqneodriver.aq_tools import aq_jobs_to_cypher
# from tqdm.auto import tqdm
# from aqneodriver.loggers import logger
#
# @dataclass
# class JobsQuery:
#     n_samples: int = MISSING
#     user: Optional[str] = None
#
#
# @dataclass
# class UpdateJobs(Task):
#     """Populates the Neo4j graph db with Aquarium inventory.
#
#
#     Update the following relationships:
#
#     ::
#
#         (a:Sample) -[hasItem]-> (b:Item)
#         (a:Item) -[hasSample]-> (b:Sample)
#         (a:Item) -[hasObjectType]-> (b:ObjectType)
#         (a:Sample) -[partOf]-> (b:Collection)
#         (a:Collection) -[hasSample]-> (b:Sample)
#
#     .. warning::
#
#         This query often takes a **very** long time
#     """
#
#     name: str = "update_jobs"  #: the task name
#     n_jobs: Optional[int] = None  #: number of parallel jobs to run
#     chunksize: int = 100  #: chunksize for each parallel job (default: 100)
#     strict: bool = True  #: if False, will catch ConstraintErrors if they arise
#     query: JobsQuery = JobsQuery()
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
