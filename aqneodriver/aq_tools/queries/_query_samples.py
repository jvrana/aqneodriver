from ._abstract_interface import RecursiveRelationshipQuery
from pydent import ModelBase
from pydent.models import FieldValue, Sample, SampleType, FieldType
from pydent import Browser
from typing import Tuple, Dict, Generator, Any, List


class QueryAquariumSamples(RecursiveRelationshipQuery):
    """
    Query Aquarium for sample relationships.
    """

    @classmethod
    def get_models(
            cls,
            _: Browser,
            model: ModelBase,
    ) -> Generator[Tuple[ModelBase, Dict[str, Any]], None, None]:
        if isinstance(model, Sample):
            for fv in model.field_values:
                yield fv, {cls.EDGETYPE: "hasFieldValue"}
            yield model.sample_type, {cls.EDGETYPE: "hasSampleType"}
        elif isinstance(model, FieldValue):
            if model.parent_class == "Sample":
                parent = model.get_parent()
                field_type = parent.safe_get_field_type(model)
            else:
                field_type = model.field_type
            if field_type:
                yield field_type, {cls.EDGETYPE: "hasFieldType"}

                if field_type.ftype == "sample" and model.sample:
                    yield model.sample, {cls.EDGETYPE: "hasSample"}
        elif isinstance(model, SampleType):
            for ft in model.field_types:
                yield ft, {cls.EDGETYPE: "hasFieldType"}

    @classmethod
    def cache_func(cls, browser: Browser, models: List[ModelBase]) -> None:
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


aq_samples_to_cypher = QueryAquariumSamples()
