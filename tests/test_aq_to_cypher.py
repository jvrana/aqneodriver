from pydent import AqSession

from aqneodriver.structured_queries.aquarium import aq_jobs_to_cypher
from aqneodriver.structured_queries.aquarium import aq_samples_to_cypher


def test_aq_samples_to_cypher(aq: AqSession):

    models = aq.Sample.last(10)
    models += aq.Item.last(10)
    payloads = aq_samples_to_cypher(aq=aq, models=models)
    for p, pdata in payloads:
        print(p)
        print(len(pdata["datalist"]))
        print(pdata)


def test_aq_jobs(aq: AqSession):

    models = aq.Sample.last(1)
    payloads = aq_jobs_to_cypher(aq=aq, models=models)
    for p, pdata in payloads:
        print(p)
        print(len(pdata["datalist"]))
        print(pdata)
