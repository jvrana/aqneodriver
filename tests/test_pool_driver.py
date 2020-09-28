from pydent import AqSession

from aqneodriver.driver import AquariumETLDriver
from aqneodriver.structured_queries.aquarium import aq_samples_to_cypher


def test_pooled(aq: AqSession, etl: AquariumETLDriver):
    etl.clear()
    models = aq.Sample.last(10)
    node_payloads = aq_samples_to_cypher(aq, models)
    etl.pool(12).write(node_payloads)
