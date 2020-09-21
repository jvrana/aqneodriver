from omegaconf import DictConfig
import hydra
from aqneodriver.tasks import Task
from aqneodriver.config import init_config_store

init_config_store()


@hydra.main(config_path="../conf", config_name="config")
def main(cfg: DictConfig):
    Task.run_task(cfg)
