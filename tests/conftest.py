import pytest
from os.path import abspath, dirname, join
from aqneoetl.config import get_config
from aqneoetl.etl import AquariumETL
from aqneoetl.etl import logger

from pydent import AqSession

here = dirname(abspath(__file__))


@pytest.fixture(autouse=True)
def set_loglevel():
    logger.setLevel("ERROR")


@pytest.fixture
def fixtures():
    return join(here, 'fixtures')


@pytest.fixture
def config(fixtures):
    return get_config(directory=fixtures)

@pytest.fixture
def aq(config):
    session = AqSession(config.aquarium.login, config.aquarium.password, config.aquarium.host)
    yield session


@pytest.fixture
def etl(config, aq):
    etl = AquariumETL(config.neo.uri, config.neo.user, config.neo.password)
    etl.clear()
    models = aq.Sample.last(100)
    for m in models:
        etl.create(m)
    yield etl
    # etl.clear()