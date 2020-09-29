from dataclasses import dataclass, field
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
    SET n.stub = false
    RETURN n.id AS x
    ORDER BY x
    """


@dataclass
class GetSamples(StructuredCypherQuery):
    limit: int
    query = """
    MATCH (n:Sample) RETURN n LIMIT {limit}
    """

@dataclass
class AutoStub(StructuredCypherQuery):
    m1: str
    m2: str
    ref: str
    etype: str
    m1_props: dict = field(default_factory=lambda: {})

    query = """
    MATCH (b:{m2} {m1_props})
    MERGE (a:{m1} { id: b.{ref} })
    ON CREATE SET
        a.stub = true
    RETURN a
    """

    @classmethod
    def from_string(cls, rstr: str, etype: str, m1_props=None):
        def stripsplit(s, p):
            return [x.strip() for x in s.split(p)]
        part1, part2 = stripsplit(rstr, '=')
        m1, k1 = stripsplit(part1, '.')
        m2, k2 = stripsplit(part2, '.')
        if k1 == 'id':
            model1 = m1
            model2 = m2
            ref = k2
        elif k2 == 'id':
            model1 = m2
            model2 = m1
            ref = k1
        m1_props = m1_props or dict()
        return cls(model1, model2, ref, etype, m1_props=m1_props)


@dataclass
class AutoRelationship(AutoStub):
    query = """
    MATCH (b:{m2} {m1_props})
    WHERE b.{m2}
    MERGE (a:{m1} { id: b.{ref} })
    ON CREATE SET a.stub = true
    MERGE (a) -[r:{etype}]-> (b)
    RETURN a, r, b
    """

