from pydent import AqSession

from aqneoetl.driver import AquariumETLDriver
from aqneoetl.queries import aq_to_cypher


def test_pooled(aq: AqSession, etl: AquariumETLDriver):
    etl.clear()
    models = aq.Sample.last(10)
    node_payloads, edge_payloads = aq_to_cypher(aq, models)

    etl.pool(12).write(node_payloads)
    results = etl.pool(12).write(edge_payloads)
    print(results)
    assert isinstance(results, list)
    example = results[0]
    assert isinstance(example, list)
    assert len(example) == 1
    assert len(example[0]) == 3
    assert isinstance(example[0][0], int)
    assert isinstance(example[0][2], int)