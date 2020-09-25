from os.path import abspath
from os.path import dirname
from os.path import join

import pytest
from pydent import AqSession

from aqneodriver.config import get_config
from aqneodriver.driver import AquariumETLDriver
from aqneodriver.driver import logger

here = dirname(abspath(__file__))


@pytest.fixture(autouse=True)
def set_loglevel():
    logger.setLevel("ERROR")


@pytest.fixture
def app_dir():
    return join(here, '..')


@pytest.fixture
def config_dir():
    return join(here, "..")


@pytest.fixture
def config(config_dir):
    return get_config(directory=config_dir, config_name="test_config")


@pytest.fixture
def aq(config):
    session = AqSession(
        config.aquarium.user, config.aquarium.password, config.aquarium.host
    )
    yield session


@pytest.fixture
def etl(config, aq):
    etl = AquariumETLDriver(config.neo.uri, config.neo.user, config.neo.password)
    etl.clear()
    models = aq.Sample.last(100)
    for m in models:
        etl.aq_create(m)
    yield etl
    # etl.clear()
