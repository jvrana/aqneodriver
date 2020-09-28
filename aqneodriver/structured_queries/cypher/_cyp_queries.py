from dataclasses import dataclass
from typing import List

from ._struct_cyp_query import StructuredCypherQuery


@dataclass
class MergeModels(StructuredCypherQuery):
    datalist: List[dict]
    query = """
    UNWIND $datalist AS data
    MERGE (n: {model_type} { id: data.id } )
    ON CREATE SET n = data
    ON MATCH SET n += data
    RETURN n.id AS x
    ORDER BY x
    """


@dataclass
class GetSamples(StructuredCypherQuery):
    limit: int
    query = """
    MATCH (n:Sample) RETURN n LIMIT {limit}
    """
