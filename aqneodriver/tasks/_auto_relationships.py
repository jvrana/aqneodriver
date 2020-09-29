from dataclasses import dataclass

from omegaconf import DictConfig
from rich.progress import Progress

from ._task import Task
from aqneodriver.structured_queries.cypher._auto_relationships import get_auto_relationships


@dataclass
class AutoRelationshipsTask(Task):
    name: str = "auto_relations"

    def run(self, cfg: DictConfig):
        etl, aq = self.sessions(cfg)
        with Progress() as progress:
            payloads = get_auto_relationships()
            task0 = progress.add_task("writing nodes...", total=len(payloads))
            for payload in payloads:
                etl.write(payload)
                print(payload[0])
                progress.update(task0, advance=1)