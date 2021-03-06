import pytest
from omegaconf import DictConfig
from omegaconf import MissingMandatoryValue
from omegaconf import OmegaConf

from aqneodriver.tasks import UpdateInventory
from aqneodriver.tasks._update_inventory import InventoryQuery


def test_update_inventory(config: DictConfig):
    """Since we did not provide the Query and n_samples is required, we should
    recieve an error."""
    task = UpdateInventory(
        n_jobs=12, strict=False, log_level="DEBUG", query=InventoryQuery(n_samples=100)
    )
    task(config)
