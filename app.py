import hydra
from omegaconf import OmegaConf


@hydra.main(config_path='conf', config_name='config')
def app(cfg):
    print(OmegaConf.to_yaml(cfg))
    ports = cfg.docker.services.neo4j.ports
    port_mapping = {}
    for p in ports:
        _from, _to = p.split(':')
        port_mapping[_from] = _to
    cfg.neo.port = port_mapping['7687']

if __name__ == '__main__':
    app()
