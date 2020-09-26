from typing import List
from typing import Optional

import networkx as nx
from pydent import Browser
from pydent import ModelBase
from pydent.exceptions import ForbiddenRequestError

from ._types import CacheFuncCallable
from ._types import GetModelsCallable
from ._types import KeyFuncCallable
from ._types import NewEdgeCallback
from ._types import NewNodeCallback


def relationship_network(
    browser: Browser,
    models: List[ModelBase],
    get_models: GetModelsCallable,
    cache_func: Optional[CacheFuncCallable] = None,
    key_func: Optional[KeyFuncCallable] = None,
    reverse: bool = False,
    g: Optional[nx.DiGraph] = None,
    strict_cache: bool = True,
    new_node_callback: Optional[NewNodeCallback] = None,
    new_edge_callback: Optional[NewEdgeCallback] = None,
):
    """Build a DAG of related models based on some relationships. By default
    are built from a model using (model.__class__.__name__, model._primary_key)

    .. seealso::
        Usage example :meth:`sample_network <pydent.browser.Browser.sample_network>`

    :param browser:
    :param models: list of models
    :param get_models: A function that takes in a single instance of a Model
        and returns an iterable of Tuples of (model, data),
        which is used to build edges.
    :param key_func: An optional function that takes in a single instance of
        a Model and returns a key and some node data
    :param cache_func: A function that takes in a list of models and caches
        some results.
    :param g: optional nx.DiGraph
    :param reverse: whether to reverse the edge list
    :param strict_cache: if True, if a request occurs after the cache step,
        a ForbiddenRequestException will be raised.
    :param
    :param new_edge_callback:
    :param new_node_callback:
    :return: the relationship graph
    """

    def add_node(g, key, ndata):
        g.add_node(key, attr_dict=ndata)
        if new_node_callback:
            new_node_callback(key, ndata)

    def add_edge(g, k1, k2, edata):
        g.add_edge(k1, k2, attr_dict=edata)
        if new_edge_callback:
            new_edge_callback(k1, k2, edata)

    if key_func is None:

        def key_func(m):
            return (m.__class__.__name__, m._primary_key), {}

    if g is None:
        g = nx.DiGraph()

    models = list(models)

    if not models:
        return g

    for m in models:
        key, ndata = key_func(m)
        add_node(g, key, ndata)

    visited = list(g.nodes)

    if cache_func:
        cache_func(browser, models)

    new_models = []
    new_edges = []

    if strict_cache:
        kwargs = {"using_requests": False, "session_swap": True}
    else:
        kwargs = {}

    with browser.session(**kwargs):
        try:
            for m1 in models:
                for m2, data in get_models(browser, m1):
                    if key_func(m2)[0] not in visited:
                        new_models.append(m2)
                    new_edges.append((key_func(m2)[0], key_func(m1)[0], data))
        except ForbiddenRequestError as e:
            msg = (
                "An exception occurred while strict_cache == True.\n"
                "This is most likely due to the cache_func not being thorough.\n"
                "{}".format(str(e))
            )
            raise e.__class__(msg)

    for n1, n2, edata in new_edges:
        if reverse:
            add_edge(g, n2, n1, edata)
        else:
            add_edge(g, n1, n2, edata)

    return relationship_network(
        browser,
        new_models,
        get_models,
        cache_func,
        key_func=key_func,
        g=g,
        reverse=reverse,
        strict_cache=strict_cache,
        new_node_callback=new_node_callback,
        new_edge_callback=new_edge_callback,
    )
