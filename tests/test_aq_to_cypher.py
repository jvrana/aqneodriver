from pydent import AqSession

from aqneodriver.aq_tools import aq_samples_to_cypher

def test_aq_samples_to_cypher(aq: AqSession):

    models = aq.Sample.last(10)
    node_payloads, edge_payloads = aq_samples_to_cypher(aq=aq, models=models)
    pass
