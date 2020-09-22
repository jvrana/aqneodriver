import pytest
from omegaconf import DictConfig, MissingMandatoryValue

from aqneodriver.tasks import UpdateSampleDatabase
from aqneodriver.tasks._update_samples import Query
from omegaconf import OmegaConf


def test_update_cfg(config: DictConfig):
    """This tests whether we can update the configuration
    and still get structured validation."""
    OmegaConf.set_struct(config, False)
    OmegaConf.update(config, 'task', UpdateSampleDatabase(), merge=False)
    OmegaConf.set_struct(config, True)
    query = config.task.query
    with pytest.raises(MissingMandatoryValue):
        query.n_samples


def test_update_task(config: DictConfig):
    task = UpdateSampleDatabase(n_jobs=12, strict=False, log_level="DEBUG", query=Query(n_samples=10))
    task(config)


def test_update_task_missing_query(config: DictConfig):
    """Since we did not provide the Query and n_samples is required,
    we should recieve an error."""
    task = UpdateSampleDatabase(n_jobs=12, strict=False, log_level="DEBUG")
    with pytest.raises(MissingMandatoryValue):
        task(config)
