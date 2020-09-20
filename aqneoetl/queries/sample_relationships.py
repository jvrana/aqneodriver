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
from pydent.models import SampleType

from ._relationship_network import NewEdgeCallback
from ._relationship_network import NewNodeCallback
from ._relationship_network import relationship_network
from aqneoetl.types import FormatData
from aqneoetl.types import Payload
from aqneoetl.utils import format_cypher_query

TYPE = "type"


def _key_func(model: ModelBase) -> Tuple[Tuple[str, int], Dict[str, ModelBase]]:
    name = model.get_server_model_name()
    return (name, model.id), {"model": model}


def _get_models(
    _: Browser,
    model: ModelBase,
) -> Generator[Tuple[ModelBase, Dict[str, Any]], None, None]:
    if isinstance(model, Sample):
        for fv in model.field_values:
            yield fv, {TYPE: "hasFieldValue"}
        yield model.sample_type, {TYPE: "hasSampleType"}
    elif isinstance(model, FieldValue):
        if model.parent_class == "Sample":
            parent = model.get_parent()
            field_type = parent.safe_get_field_type(model)
        else:
            field_type = model.field_type
        if field_type:
            yield field_type, {TYPE: "hasFieldType"}

            if field_type.ftype == "sample" and model.sample:
                yield model.sample, {TYPE: "hasSample"}
    elif isinstance(model, SampleType):
        for ft in model.field_types:
            yield ft, {TYPE: "hasFieldType"}


def _cache_func(browser: Browser, models: List[ModelBase]) -> None:
    """Cache function to reduce queries.

    This is kind of difficult to code correctly and requires try-and-
    error. It helps to look at the `get_models` function and see where
    implicit requests are happening.
    """
    samples = [m for m in models if isinstance(m, Sample)]
    browser.get(samples, {"sample_type": "field_types", "field_values": {}})

    field_values = [m for m in models if isinstance(m, FieldValue)]
    browser.get(
        field_values,
        {
            "sample": {},
            "parent_sample": {"sample_type": "field_types"},
            "field_type": {},
        },
    )

    sample_types = [m for m in models if isinstance(m, SampleType)]
    browser.get(sample_types, {"field_types": {"sample_type": "field_types"}})

    field_types = [m for m in models if isinstance(m, FieldType)]
    browser.get(field_types, {"sample_type": {}})


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
            strict_cache=True,
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


def aq_to_cypher(
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
