from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
import re


class AquariumETL:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        try:
            self.setup()
        except ClientError:
            pass

    def close(self):
        self.driver.close()

    def setup(self):
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT primary_key "
                        "ON (sample:Sample) ASSERT sample.id IS UNIQUE")

    def create_constraint(self, model):
        with self.driver.session() as sess:
            sess.run()  # "CREATE CONSTRAIN {}_primary_key ".format(model.get_tableized_name())
            # "ON (m:{n} ASSERT m.{id} is UNIQUE".format(n=model.get_server_model_name(), 'id'))

    def clear(self):
        with self.driver.session() as sess:
            sess.run("match (a) -[r] -> () delete a, r")
            sess.run("match (a) delete a")

    def update(self, model, mode="MATCH"):
        with self.driver.session() as session:
            node_label = model.get_server_model_name()
            data = model.dump()
            return session.write_transaction(self._create, node_label, data, mode=mode)

    def create(self, model):
        return self.update(model, mode='CREATE')

    @staticmethod
    def _create(tx, node_label, data, mode="CREATE"):
        a = """
        $mode (a:{})
        SET a.$k
        """
        query = "{mode} (a:{label}) ".format(mode=mode, label=node_label)
        query += ''.join(["SET a.{k} = ${k} ".format(k=k) for k in data])
        query += " RETURN a"
        result = tx.run(query, **data)
        return result.single()[0]

    #     def where(self, query, on_return=None):
    #         if on_return is None:
    #             def on_return(result):
    #                 return result.values()
    #         with self.driver.session() as session:
    #             query_str = ', '.join(["{k}: ${k}".format(k=k, v=v) for k, v in query.items()])
    #             query_str = "{{ {} }}".format(query_str)
    #             q = "MATCH (n {})".format(query_str)
    #             return on_return(session.run("MATCH (n {id: $id, id: $id}) RETURN n", **query))

    @staticmethod
    def process_query(query, **kwargs):
        found_keys = set()
        for k in re.findall("\$(\w+)", query):
            print(kwargs)
            if k in kwargs:
                print(k)
                query = re.sub("\${}".format(k), str(kwargs[k]), query)
                found_keys.add(k)

        return query, found_keys

    @classmethod
    def _create_tx(cls, query, process, **kwargs):
        query, used_keys = cls.process_query(query, **kwargs)
        remaining_keys = set(kwargs).difference(used_keys)
        remaining_kwargs = {k: kwargs[k] for k in remaining_keys}

        def transaction(tx):
            result = tx.run(query, **remaining_kwargs)
            return process(result)

        return transaction

    def write(self, query, **kwargs):
        with self.driver.session() as session:
            tx = self._create_tx(query, process=lambda x: x.values(), **kwargs)
            return session.write_transaction(tx)

    def read(self, query, **kwargs):
        with self.driver.session() as session:
            tx = self._create_tx(query, process=lambda x: x.values(), **kwargs)
            return session.read_transaction(tx)
