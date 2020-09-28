# flake8: noqa
from dataclasses import dataclass
from typing import Dict
from typing import List

import pytest

from aqneodriver.exceptions import ClassDefinitionConflictError
from aqneodriver.exceptions import ImproperClassDefinitionError
from aqneodriver.exceptions import InitializationNotAllowedError
from aqneodriver.exceptions import ValidationError
from aqneodriver.structured_queries.cypher import MergeModels
from aqneodriver.structured_queries.cypher import StructuredCypherQuery
from aqneodriver.structured_queries.cypher import StructuredCypherQueryMeta


@pytest.fixture(autouse=True)
def clear_queries():
    existing_queries = set(StructuredCypherQueryMeta.queries)
    yield
    new_queries = set(StructuredCypherQueryMeta.queries).difference(existing_queries)
    for q in new_queries:
        del StructuredCypherQueryMeta.queries[q]


def test_cannot_initialize_structured_query():
    with pytest.raises(InitializationNotAllowedError):
        StructuredCypherQuery()


def test_cannot_initialize_without_query():

    with pytest.raises(ValidationError):

        class Foo(StructuredCypherQuery):
            pass


def test_missing_field():

    with pytest.raises(ValidationError):

        class Foo(StructuredCypherQuery):
            query = "$data"


def test_name_conflict():
    class Foo(StructuredCypherQuery):
        query = ""

    with pytest.raises(ClassDefinitionConflictError):

        class Foo(StructuredCypherQuery):
            query = ""


def test_str_structure_query_meta_class():
    print(str(StructuredCypherQueryMeta))


def test_str_structure_query_meta_class():
    print(repr(StructuredCypherQueryMeta))


def test_str_structure_query_class():
    print(str(StructuredCypherQuery))


def test_repr_structure_query_class():
    print(repr(StructuredCypherQuery))


def test_basic_payload():
    class Foo(StructuredCypherQuery):

        data: List[Dict]
        query = """
        MATCH (n:Sample)
        SET n += $data
        """

    data = {"id": 3, "key": "value"}
    query = Foo(data)
    query_str, query_data = query.payload()
    assert query_str.startswith("//")
    assert "\n".join(query_str.splitlines()[1:]) == "MATCH (n:Sample)\nSET n += $data"
    assert query_data == {"data": data}


class TestQueryKwargs:
    def test_kwargs_override_using_field(self):
        @dataclass
        class Foo(StructuredCypherQuery):

            data: List[Dict]
            model_type: str = "SampleType"
            query = """
            MATCH (n:{model_type})
            SET n += $data
            """

        Foo(data=4).payload()

    def test_kwargs_override_using_kwargs(self):
        @dataclass
        class Foo(StructuredCypherQuery):

            data: List[Dict]
            query = """
            MATCH (n:{model_type})
            SET n += $data
            """

        with pytest.raises(ValueError):
            Foo(data=4).payload()
        Foo(data=4).payload(model_type="SampleType")

    def test_kwargs_inherit(self):
        @dataclass
        class Foo(StructuredCypherQuery):

            data: List[Dict]
            query = """
            MATCH (n:{model_type})
            SET n += $data
            """

        @dataclass
        class Bar(Foo):
            model_type: str = "SampleType"

        with pytest.raises(ValueError):
            Foo(data=4).payload()
        Bar(data=4).payload()


def test_basic_payload():
    class Foo(StructuredCypherQuery):

        data: List[Dict]
        query = """
        MATCH (n:Sample)
        SET n += $data
        """

    data = {"id": 3, "key": "value"}
    query = Foo(data)
    query_str, query_data = query.payload()
    assert query_str.startswith("//")
    assert "\n".join(query_str.splitlines()[1:]) == "MATCH (n:Sample)\nSET n += $data"
    assert query_data == {"data": data}


class TestQueries:
    def test_create_models(self, aq, etl):
        items = aq.Item.last(10)
        datalist = [i.dump() for i in items]
        results = etl.write(*MergeModels(datalist=datalist).payload(model_type="Item"))
        print(results)
