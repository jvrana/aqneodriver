from abc import abstractmethod
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Optional
from typing import Tuple

import networkx as nx
from pydent import AqSession
from pydent import Browser
from pydent import ModelBase

from ._abc_query import AquariumQuery
from aqneodriver.aq_tools._relationship_network import relationship_network
from aqneodriver.aq_tools._types import NewEdgeCallback
from aqneodriver.aq_tools._types import NewNodeCallback
from aqneodriver.payload import Payload
from aqneodriver.utils.abstract_interface import abstract_interface


@abstract_interface
class RecursiveRelationshipQuery(AquariumQuery):
    """Abstract base class for querying Aquarium. Provide a `get_models` and
    `cache_func`. Optionally, the `key_func` may be re-defined.

    Usage:

    .. code-block::

        class MyQuery(AquariumRelationshipQuery):

            @classmethod
            def get_model(cls, browser, model):
                ...

            @classmethod
            def cache_func(cls, browser, models):
                ...

        query = MyQuery()
        query(aqsession, models, node_callback, edge_callback))
    """

    EDGETYPE = "type"  #: key to store the edge label in the neo graph db

    @classmethod
    @abstractmethod
    def get_models(
        cls, browser: Browser, model
    ) -> Generator[Tuple[ModelBase, Dict[str, Any]], None, None]:
        pass

    @classmethod
    @abstractmethod
    def cache_func(cls, browser: Browser, models: List[ModelBase]) -> None:
        pass

    @staticmethod
    def key_func(model: ModelBase) -> Tuple[Tuple[str, int], Dict[str, ModelBase]]:
        name = model.get_server_model_name()
        return (name, model.id), {"model": model}

    def _network_to_cypher_queries(
        self,
        g: nx.DiGraph,
    ) -> Tuple[List[Payload], List[Payload]]:
        """Convert a :class:`nx.DiGraph` to a tuple of cypher payloads.

        :param g:
        :return:
        """
        # TODO: initialize node create queries
        node_creation_queries = []
        edge_creation_queries = []

        for n, ndata in g.nodes(data=True):
            model: ModelBase = ndata["attr_dict"]["model"]
            node_creation_queries.append(self._create_node_query(model))

        for n1, n2, edata in g.edges(data=True):
            model1: ModelBase = ndata["attr_dict"]["model"]
            model2: ModelBase = ndata["attr_dict"]["model"]
            query = self._create_edge_query(
                model1, model2, edata["attr_dict"][self.EDGETYPE]
            )
            edge_creation_queries.append((query, {}))

        node_payloads = [Payload(*q) for q in node_creation_queries]
        edge_payloads = [Payload(*q) for q in edge_creation_queries]
        return node_payloads, edge_payloads

    @classmethod
    def _create_network(
        cls,
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
                get_models=cls.get_models,
                cache_func=cls.cache_func,
                key_func=cls.key_func,
                strict_cache=False,
                new_node_callback=new_node_callback,
                new_edge_callback=new_edge_callback,
            )
            return g

    def run(
        self,
        aq: AqSession,
        models: List[ModelBase],
        new_node_callback: Optional[NewNodeCallback] = None,
        new_edge_callback: Optional[NewEdgeCallback] = None,
    ):
        """Execute the aquarium query, recursively finding relationships.

        :param aq: The :class:`pydent.aqsession.AqSession` instance to use
        :param models: List of Aquarium models to begin the recursive search
        :param new_node_callback: Callback to be called on each (node, ndata) tuple
                                  (:code:`Tuple[Hashable, Dict[str, Any]`)
        :param new_edge_callback: Callback to be called on each (node1, node2, edata) tuple
                                  (:code:`Tuple[Hashable, Hashable, Dict[str, Any]`)
        :return: tuple of (node_payloads, edge_payloads)
        """
        graph = self._create_network(
            aq,
            models,
            new_node_callback=new_node_callback,
            new_edge_callback=new_edge_callback,
        )
        node_payloads, edge_payloads = self._network_to_cypher_queries(graph)
        return node_payloads, edge_payloads
