from abc import abstractmethod
from typing import List
from typing import Optional
from typing import Tuple

from pydent import AqSession
from pydent import ModelBase

from aqneodriver.aq_tools._types import NewEdgeCallback
from aqneodriver.aq_tools._types import NewNodeCallback
from aqneodriver.utils import format_cypher_query
from aqneodriver.utils.abstract_interface import abstract_interface
from aqneodriver.utils.abstract_interface import AbstractInterface


# TODO: refactor queries
@abstract_interface
class AquariumQuery(AbstractInterface):
    @abstractmethod
    def run(
        self,
        aq: AqSession,
        models: List[ModelBase],
        new_node_callback: Optional[NewNodeCallback] = None,
        new_edge_callback: Optional[NewEdgeCallback] = None,
    ):
        pass

    def _create_node_query(self, model: ModelBase) -> Tuple[str, dict]:
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
        return query, data

    def _create_edge_query(
        self, model1: ModelBase, model2: ModelBase, edge_type: str
    ) -> Tuple[str, dict]:
        query = format_cypher_query(
            """
            MATCH (a:{type1}),(b:{type2})
            WHERE a.id = {id1} AND b.id = {id2}
            CREATE (a)-[r:{etype}]->(b)
            RETURN a.id, r.id, b.id
            """,
            type1=model1.get_server_model_name(),
            id1=model1.id,
            type2=model2.get_server_model_name(),
            id2=model2.id,
            etype=edge_type,
        )
        return query

    def __call__(
        self,
        aq: AqSession,
        models: List[ModelBase],
        new_node_callback: Optional[NewNodeCallback] = None,
        new_edge_callback: Optional[NewEdgeCallback] = None,
    ):
        return self.run(aq, models, new_node_callback, new_edge_callback)
