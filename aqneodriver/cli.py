import hydra
from omegaconf import DictConfig

from aqneodriver.config import init_config_store
from aqneodriver.tasks import Task

init_config_store()


@hydra.main(config_path="../conf", config_name="config")
def main(cfg: DictConfig):
    Task.run_task(cfg)
