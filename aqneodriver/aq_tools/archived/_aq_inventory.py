from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Tuple

import networkx as nx
from pydent import AqSession
from pydent.base import ModelBase
from pydent.browser import Browser
from pydent.models import FieldType
from pydent.models import FieldValue
from pydent.models import Sample
from pydent.models import ObjectType
from pydent.models import Item, Collection

from ._tools import NewEdgeCallback
from ._tools import NewNodeCallback
from ._tools import relationship_network
from aqneodriver.types import FormatData
from aqneodriver.types import Payload
from aqneodriver.utils import format_cypher_query

TYPE = "type"


def _key_func(model: ModelBase) -> Tuple[Tuple[str, int], Dict[str, ModelBase]]:
    name = model.get_server_model_name()
    return (name, model.id), {"model": model}


def _get_models(
    _: Browser,
    model: ModelBase,
) -> Generator[Tuple[ModelBase, Dict[str, Any]], None, None]:
    if isinstance(model, Sample):
        for item in model.items:
            yield item, {TYPE: "hasItem"}
        yield model.sample_type, {TYPE: "hasSampleType"}
    elif isinstance(model, Item):
        yield model.object_type, {TYPE: "hasObjectType"}
        if model.sample:
            yield model.sample, {TYPE: "hasSample"}
    elif isinstance(model, ObjectType):
        yield model.sample_type, {TYPE: "hasSampleType"}




def _cache_func(browser: Browser, models: List[ModelBase]) -> None:
    """Cache function to reduce queries.

    This is kind of difficult to code correctly and requires try-and-
    error. It helps to look at the `get_models` function and see where
    implicit requests are happening.
    """
    samples = [m for m in models if isinstance(m, Sample)]
    browser.get(samples, {"items": {'sample': {'sample_type': {}}, 'object_type': {'sample_type': {}}}})

    items = [m for m in models if isinstance(m, Item)]
    browser.get(
        items,
        {
            "sample": {'sample_type': {}}
        },
    )



def _create_sample_network(
    aq: AqSession,
    models: List[ModelBase],
    new_node_callback: NewNodeCallback = None,
    new_edge_callback: NewEdgeCallback = None,
) -> nx.DiGraph:

    with aq.with_cache(timeout=120) as sess:
        browser: Browser = sess.browser
        browser.clear()
        browser.update_cache(models)
        g = relationship_network(
            sess.browser,
            models,
            reverse=True,
            get_models=_get_models,
            cache_func=_cache_func,
            key_func=_key_func,
            strict_cache=False,
            new_node_callback=new_node_callback,
            new_edge_callback=new_edge_callback,
        )
        return g


def _sample_network_to_cypher_queries(
    g: nx.DiGraph,
) -> Tuple[
    List[Tuple[str, Dict[str, FormatData]]], List[Tuple[str, Dict[str, FormatData]]]
]:
    # TODO: initialize node create queries
    node_creation_queries = []
    edge_creation_queries = []

    for n, ndata in g.nodes(data=True):
        model: ModelBase = ndata["attr_dict"]["model"]
        data = model.dump()
        query = format_cypher_query(
            """
        CREATE (n:{type})
        SET n.{key} = ${key}
        RETURN n
        """,
            type=model.get_server_model_name(),
            key=list(data),
        )
        node_creation_queries.append((query, data))

    for n1, n2, edata in g.edges(data=True):
        query = format_cypher_query(
            """
        MATCH (a:{type1}),(b:{type2})
        WHERE a.id = {id1} AND b.id = {id2}
        CREATE (a)-[r:{etype}]->(b)
        RETURN a.id, r.id, b.id
        """,
            type1=n1[0],
            id1=n1[1],
            type2=n2[0],
            id2=n2[1],
            etype=edata["attr_dict"][TYPE],
        )

        edge_creation_queries.append((query, {}))
    return node_creation_queries, edge_creation_queries


def aq_samples_to_cypher(
    aq: AqSession,
    models: List[ModelBase],
    new_node_callback: NewNodeCallback = None,
    new_edge_callback: NewEdgeCallback = None,
) -> Tuple[List[Payload], List[Payload]]:
    graph = _create_sample_network(
        aq,
        models,
        new_node_callback=new_node_callback,
        new_edge_callback=new_edge_callback,
    )
    q1, q2 = _sample_network_to_cypher_queries(graph)
    node_payloads = [Payload(*q) for q in q1]
    edge_payloads = [Payload(*q) for q in q2]
    return node_payloads, edge_payloads
