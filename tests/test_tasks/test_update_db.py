from omegaconf import DictConfig

from aqneodriver.tasks import UpdateDatabase


def test_update_task(config: DictConfig):
    task = UpdateDatabase(n_jobs=12, strict=False)
    task(config)
