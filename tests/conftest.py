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
def fixtures():
    return join(here, "fixtures")


@pytest.fixture
def config(fixtures):
    return get_config(directory=fixtures)


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
