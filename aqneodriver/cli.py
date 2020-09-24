import hydra
from omegaconf import DictConfig

from aqneodriver.config import init_config_store
from aqneodriver.tasks import Task
from rich.traceback import install
from rich.console import Console
from aqneodriver.exceptions import HelpException
from rich import print

console = Console()
install()

init_config_store()


@hydra.main(config_path="../conf", config_name="config")
def main(cfg: DictConfig):
    try:
        Task.run_task(cfg)
    except HelpException:
        pass  # just ignore help exceptions
    except Exception:
        console.print_exception()
