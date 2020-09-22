from omegaconf import DictConfig

from aqneodriver.tasks import UpdateJobs
from aqneodriver.tasks._update_jobs import JobsQuery


def test_update_inventory(config: DictConfig):
    """Since we did not provide the Query and n_samples is required,
    we should recieve an error."""
    task = UpdateJobs(n_jobs=12, strict=False, log_level="DEBUG", query=JobsQuery(n_samples=100))
    task(config)