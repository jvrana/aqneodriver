from abc import abstractmethod
from typing import List
from typing import Optional

from pydent import AqSession
from pydent import ModelBase

from aqneodriver.payload import Payload
from aqneodriver.structured_queries.aquarium._types import NewNodeCallback
from aqneodriver.structured_queries.cypher import MergeModels
from aqneodriver.utils.abstract_interface import abstract_interface
from aqneodriver.utils.abstract_interface import AbstractInterface


def dump(m: ModelBase) -> dict:
    data = {k: v for k, v in m._get_data().items() if k not in m.fields}
    return data


# TODO: refactor queries
@abstract_interface
class StructuredAquariumQuery(AbstractInterface):

    DEFAULT_WRITE_MODE = "CREATE"

    @staticmethod
    def models_to_payloads(models: List[ModelBase]) -> List[Payload]:
        grouped_by_class = {}
        for m in models:
            grouped_by_class.setdefault(m.get_server_model_name(), list())
            grouped_by_class[m.get_server_model_name()].append(m)

        payloads = []
        for model_type, model_list in grouped_by_class.items():
            query_str, query_data = MergeModels(
                datalist=[dump(m) for m in model_list]
            ).payload(model_type=model_type)
            payloads.append(Payload(query_str, query_data))
        return payloads

    @abstractmethod
    def run(
        self,
        aq: AqSession,
        models: List[ModelBase],
        *,
        new_node_callback: Optional[NewNodeCallback] = None
    ) -> List[ModelBase]:
        pass

    def __call__(
        self,
        aq: AqSession,
        models: List[ModelBase],
        *,
        new_node_callback: Optional[NewNodeCallback] = None
    ):
        models = self.run(aq, models, new_node_callback=new_node_callback)
        return self.models_to_payloads(models)
