from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import Tuple

from pydent import Browser
from pydent import ModelBase
from pydent.models import Item
from pydent.models import ObjectType
from pydent.models import Sample
from pydent.models import SampleType

from ._abc_recursive_query import RecursiveRelationshipQuery


# TODO: handle collections
class QueryAquariumInventory(RecursiveRelationshipQuery):
    """Query Aquarium for item relationships."""

    @classmethod
    def get_models(
        cls,
        _: Browser,
        model: ModelBase,
    ) -> Generator[Tuple[ModelBase, Dict[str, Any]], None, None]:
        if isinstance(model, Sample):
            for item in model.items:
                yield item, {cls.EDGETYPE: "hasItem"}
            yield model.sample_type, {cls.EDGETYPE: "hasSampleType"}
        elif isinstance(model, Item):
            yield model.sample, {cls.EDGETYPE: "hasSample"}
            yield model.object_type, {cls.EDGETYPE: "hasObjectType"}

    @classmethod
    def cache_func(cls, browser: Browser, models: List[ModelBase]) -> None:
        """Cache function to reduce queries.

        This is kind of difficult to code correctly and requires try-
        and- error. It helps to look at the `get_models` function and
        see where implicit requests are happening.
        """
        samples = [m for m in models if isinstance(m, Sample)]
        items = [m for m in models if isinstance(m, Item)]
        object_types = [m for m in models if isinstance(m, ObjectType)]
        sample_types = [m for m in models if isinstance(m, SampleType)]

        browser.get(samples, {"items": {"object_type", "sample"}, "sample_type": {}})


aq_inventory_to_cypher = QueryAquariumInventory()
