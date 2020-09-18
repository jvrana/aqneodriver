from pydent import AqSession

from aqneoetl.etl import AquariumETL
from aqneoetl.queries import aq_to_cypher


def test_pooled(aq: AqSession, etl: AquariumETL):
    etl.clear()
    models = aq.Sample.last(10)
    queries = aq_to_cypher(aq, models)
    print(queries[:10])
    results = etl.pool(12).write(queries)
    print(results)
    assert isinstance(results, list)
    example = results[0]
    assert isinstance(example, list)
    assert len(example) == 1
    assert len(example[0]) == 3
    assert isinstance(example[0][0], int)
    assert isinstance(example[0][2], int)
