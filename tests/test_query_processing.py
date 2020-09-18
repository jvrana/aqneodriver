from aqneoetl.query import format_cypher_query


def test_basic_formatting():
    query = """
    MATCH (n:{label})
    """
    expected = "MATCH (n:mylabel)"
    result = format_cypher_query(query, label="mylabel")
    assert result == expected


def test_basic_formatting_with_int():
    query = """
    MATCH (n:{label})
    """
    expected = "MATCH (n:123)"
    result = format_cypher_query(query, label=123)
    assert result == expected


def test_query_list_of_keys():
    query = """
    MATCH (n:{label})
    key: {key}
    """
    expected = "MATCH (n:mylabel)\nkey: key1\nkey: key2"
    result = format_cypher_query(query, label="mylabel", key=["key1", "key2"])
    assert result == expected


def test_query_list_of_keys_mixed():
    query = """
    MATCH (n:{label})
    key: {key} {val}
    """
    expected = "MATCH (n:mylabel)\nkey: key1 value\nkey: key2 value"
    result = format_cypher_query(
        query, label="mylabel", key=["key1", "key2"], val="value"
    )
    assert result == expected


def test_query_list_of_items():
    query = """
    MATCH (n:{label})
    item: {item[0]}: {item[1]}
    """
    expected = "MATCH (n:mylabel)\nitem: key1: val1\nitem: key2: val2\nitem: key3: 3"
    data = {"key1": "val1", "key2": "val2", "key3": 3}
    result = format_cypher_query(query, label="mylabel", item=list(data.items()))
    assert result == expected
