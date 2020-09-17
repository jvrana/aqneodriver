import pytest
from os.path import abspath, dirname, join
from aqneoetl.config import get_config

here = dirname(abspath(__file__))


@pytest.fixture
def fixtures():
    return join(here, 'fixtures')

@pytest.fixture
def config(fixtures):
    get_config(directory=fixtures)


def test_config(config):
    print(config)