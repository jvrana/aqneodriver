from dataclasses import dataclass

from omegaconf import DictConfig
from rich.progress import Progress

from ._task import Task
from aqneodriver.structured_queries.cypher import get_relationships_queries


@dataclass
class UpdateRelationships(Task):

    name: str = "update_relations"

    def run(self, cfg: DictConfig):
        etl, aq = self.sessions(cfg)
        with Progress() as progress:
            queries = get_relationships_queries()
            payloads = [(q, {}) for q in queries]
            task0 = progress.add_task("writing relationships...", total=len(payloads))
            for query in queries:
                progress.update(task0, advance=1)
                etl.write(query)