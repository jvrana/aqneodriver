from omegaconf import OmegaConf

from aqneoetl.driver import AquariumETLDriver


def test_config(config):
    assert config
    print(OmegaConf.to_yaml(config))


def test_etl(config):
    AquariumETLDriver(config.neo.uri, config.neo.user, config.neo.password)


def test_update(aq, etl):
    for m in aq.Sample.last(20):
        etl.aq_update(m)