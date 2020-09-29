from pydent.relationships import HasOne, HasMany
from pydent import ModelBase, ModelRegistry
from ._cyp_queries import AutoRelationship
from typing import Type, List

from ...payload import Payload


def has_one_to_str(f):
    return "{}.{} = {}.{}".format(
        f.objtype.__name__, f.ref, f.nested, f.attr
    )


def has_many_to_str(f):
    return "{}.{} = {}.{}".format(
        f.objtype.__name__, f.attr, f.nested, f.ref
    )


def field_strs(model_type: Type[ModelBase]):
    strs = []
    if hasattr(model_type, 'fields'):
        for field in model_type.fields.values():
            if isinstance(field, HasOne):
                s = has_one_to_str(field), 'hasOne'
            elif isinstance(field, HasMany):
                s = has_many_to_str(field), 'hasMany'
            else:
                s = None
            if s is not None:
                strs.append(s)
    return list(set(strs))


def aq_model_to_auto_relationships(model_type: Type[ModelBase]) -> List[AutoRelationship]:
    qlist = []
    for query_str, name in field_strs(model_type):
        qlist.append(AutoRelationship.from_string(query_str, etype=name))
    return qlist


def get_auto_relationships() -> List[Payload]:
    payloads = []
    for model in ModelRegistry.models.values():
        for query in aq_model_to_auto_relationships(model):
            payloads.append(query.payload())
    return list(set(payloads))

    queries.append(
        AutoRelationship("Sample.sample_type_id = SampleType.id", "hasOne"),
        AutoRelationship("FieldValue.sample_type_id = SampleType.id", "hasMany"),
    )



