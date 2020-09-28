from uuid import uuid4

from omegaconf import OmegaConf

from aqneodriver.driver import AquariumETLDriver
from aqneodriver.utils import format_cypher_query


def test_config(config):
    assert config
    print(OmegaConf.to_yaml(config))


def test_etl(config):
    AquariumETLDriver(config.neo.uri, config.neo.user, config.neo.password)


def test_update(aq, etl):
    for m in aq.Sample.last(20):
        etl.aq_update(m)


def test_read(aq, etl):
    for m in aq.Sample.last(20):
        etl.aq_update(m)
    results = etl.pool(12).write([("MATCH (n:Sample) RETURN n.id LIMIT 10", {})])
    print(results)


def test_create_with_map(etl):
    """Tests creation of a node via a map."""
    uid = str(uuid4())
    write_query = format_cypher_query(
        """
        CREATE (n:Sample)
        SET n += $data
        RETURN n
        """
    )
    read_query = format_cypher_query(
        """
        MATCH (n:Sample { uid: "{uid}" })
        RETURN n
        """,
        uid=uid,
    )

    etl.write(write_query, {"data": {"uid": uid}})

    results = etl.read(read_query)
    assert results


def test_merge_with_map(etl):
    """Tests merging of node with a map."""
    uid = str(uuid4())
    write_query = format_cypher_query(
        """
        MERGE (n:Sample)
        SET n += $data
        RETURN n
        """
    )
    read_query = format_cypher_query(
        """
        MATCH (n:Sample { uid: "{uid}" })
        RETURN n
        """,
        uid=uid,
    )

    etl.write(write_query, {"data": {"uid": uid}})

    results = etl.read(read_query)
    assert results


def test_merge_update(etl):
    """Tests updating a node with a map."""
    uid = str(uuid4())
    uid2 = str(uuid4())
    write_query = format_cypher_query(
        """
        MERGE (n:Sample)
        SET n += $data
        RETURN n
        """
    )
    read_query = format_cypher_query(
        """
        MATCH (n:Sample { uid: "{uid}" })
        RETURN n
        """,
        uid=uid2,
    )
    etl.write(write_query, {"data": {"uid": uid}})
    etl.write(write_query, {"data": {"uid": uid2}})
    results = etl.read(read_query)
    assert results


def test_merge_relationship(etl):
    uid1 = str(uuid4())
    uid2 = str(uuid4())
    create_node_query = format_cypher_query(
        """
        CREATE (n: Sample)
        SET n = $data
        """
    )
    etl.write(create_node_query, {"data": {"uid": uid1}})
    etl.write(create_node_query, {"data": {"uid": uid2}})

    create_edge_query = format_cypher_query(
        """
        CREATE (a:Sample { id: $uid1 }) -[r:LABEL]-> (b:Sample { id: $uid2 })
        RETURN a, r, b"""
    )
    print(create_edge_query)

    etl.write(create_edge_query, data={"uid1": uid1, "uid2": uid2})


def test_multiple_create(aq, etl):
    query = format_cypher_query(
        """
        UNWIND $datalist AS data
        CREATE (n:Item)
        SET n += data
        RETURN n.id AS x
        ORDER BY x
        """
    )
    import time

    datalist = [i.dump() for i in aq.Item.last(1000)]

    t1 = time.time()
    etl.write(query, {"datalist": datalist})
    t2 = time.time()

    print(t2 - t1)
    # print(etl.read("MATCH (n:Item) RETURN n"))
