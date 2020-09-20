import hydra
from omegaconf import DictConfig
from pydent import AqSession
from pydent import Browser
from tqdm import tqdm

from aqneoetl.driver import AquariumETLDriver
from aqneoetl.queries import aq_to_cypher


@hydra.main(config_path="conf", config_name="config")
def main(cfg: DictConfig):
    n_samples = cfg.n_samples
    n_jobs = cfg.n_jobs
    chunksize = cfg.chunksize

    driver = AquariumETLDriver(cfg.neo.uri, cfg.neo.user, cfg.neo.password)
    aq = AqSession(cfg.aquarium.login, cfg.aquarium.password, cfg.aquarium.host)
    browser: Browser = aq.browser
    browser.log.set_level("ERROR")

    models = aq.Sample.last(n_samples)

    pbar0 = tqdm(desc="collecting aquarium models")
    node_payload, edge_payload = aq_to_cypher(
        aq, models, new_node_callback=lambda k, d: pbar0.update()
    )

    pbar1 = tqdm(desc="writing nodes", total=len(node_payload))
    driver.pool(n_jobs).write(
        node_payload, callback=lambda _: pbar1.update(), chunksize=chunksize
    )

    pbar2 = tqdm(desc="writing edges", total=len(edge_payload))
    driver.pool(n_jobs).write(
        edge_payload, callback=lambda _: pbar2.update(), chunksize=chunksize
    )


if __name__ == "__main__":
    main()
