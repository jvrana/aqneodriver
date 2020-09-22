from ._abstract_interface import RecursiveRelationshipQuery
from pydent import ModelBase
from pydent.models import Sample, SampleType, Item, ObjectType, FieldValue, Operation, Job
from pydent import Browser
from typing import Tuple, Dict, Generator, Any, List

"""
To find all jobs (non-recursively) 

1. find all samples in db
2. find all field values associated with samples (parent_class=Operation)
3. find all operations associated with field_values
4. find all items of field values
5. find all jobs of operations
"""
from pydent import AqSession


def aq_jobs_to_cypher(session: AqSession, sample_ids):
    with session.with_cache(timeout=120) as sess:
        sess.browser.where({'child_'})
        field_values: List[FieldValue] = sess.FieldValue.where({'child_sample_id': sample_ids, 'parent_class': 'Operation'})
        results = sess.browser.get('FieldFvalue', {'sample', 'operation', 'item'})
        opertions: List[Operation] = results['operation']
        jobs: List[Job] = sess.browser.get('Operation', 'jobs')
# class QueryAquariumJobs(RecursiveRelationshipQuery):
#     """
#     Query Aquarium for item relationships.
#     """
#     @staticmethod
#     def key_func(model: ModelBase) -> Tuple[Tuple[str, int], Dict[str, ModelBase]]:
#         name = model.get_server_model_name()
#         if isinstance(model, FieldValue):
#             name = "Operation" + name
#         return (name, model.id), {"model": model}
#
#     @classmethod
#     def get_models(
#             cls,
#             browser: Browser,
#             model: ModelBase,
#     ) -> Generator[Tuple[ModelBase, Dict[str, Any]], None, None]:
#         if isinstance(model, Sample):
#             if hasattr(model, '_op_fvs'):
#                 field_values = model._op_fvs
#                 for fv in field_values:
#                     if fv.role == 'input':
#                         yield fv, {cls.EDGETYPE: 'usedInInput'}
#                     elif fv.role == 'output':
#                         yield fv, {cls.EDGETYPE: 'usedInOutput'}
#                     else:
#                         raise RuntimeError('role not identified')
#         elif isinstance(model, FieldValue):
#             if model.field_type:
#                 yield model.field_type, {cls.EDGETYPE: 'hasFieldType'}
#             if model.operation:
#                 yield model.operation, {cls.EDGETYPE: 'usedInOperation'}
#             if model.sample:
#                 yield model.sample, {cls.EDGETYPE: 'hasSample'}
#             if model.item:
#                 yield model.item, {cls.EDGETYPE: 'hasItem'}
#
#         elif isinstance(model, Operation):
#             for fv in model.field_values:
#                 yield fv, {cls.EDGETYPE: 'has{}'.format(fv.role.capitalize())}
#             yield model.operation_type, {cls.EDGETYPE: 'hasOperationType'}
#
#     @classmethod
#     def cache_func(cls, browser: Browser, models: List[ModelBase]) -> None:
#         """Cache function to reduce queries.
#
#         This is kind of difficult to code correctly and requires try-and-
#         error. It helps to look at the `get_models` function and see where
#         implicit requests are happening.
#         """
#         samples = [m for m in models if isinstance(m, Sample)]
#         _fv_cache = {'operation', 'sample', 'item', 'field_type'}
#
#         browser.update_cache(samples)
#         samples_dict = {s.id: s for s in samples}
#         _field_values = browser.where({'child_sample_id': list(samples_dict), 'parent_class': 'Operation'}, 'FieldValue')
#         browser.update_cache(_field_values)
#         for fv in _field_values:
#             sample = samples_dict[fv.child_sample_id]
#             if not hasattr(sample, '_op_fvs'):
#                 sample._op_fvs = []
#             sample._op_fvs.append(fv)
#         browser.get(_field_values, _fv_cache)
#
#         field_values = [m for m in models if isinstance(m, FieldValue)]
#         operations = [m for m in models if isinstance(m, Operation)]
#
#         browser.get(field_values, _fv_cache)
#         browser.get(operations, {'field_values': _fv_cache,'operation_type': {}})
#
#
# aq_jobs_to_cypher = QueryAquariumJobs()
