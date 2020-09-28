"""Structured Aquarium queries."""
from ._struct_inv_query import StructuredInvQuery
from ._struct_jobs_query import StructuredJobQuery
from ._struct_samples_query import StructuredSamplesQuery

aq_samples_to_cypher = StructuredSamplesQuery().__call__
aq_inventory_to_cypher = StructuredInvQuery().__call__
aq_jobs_to_cypher = StructuredJobQuery().__call__


__all__ = ["aq_samples_to_cypher", "aq_inventory_to_cypher", "aq_jobs_to_cypher"]
