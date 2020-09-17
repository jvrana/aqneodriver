import functools

from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
import re
from .query import format_cypher_query
import logging
from .encrypt import Cryptography
from typing import Callable, Tuple, Any, List
from multiprocessing import Pool
from typing import TypeVar
import dill
from abc import ABC, abstractmethod
from multiprocessing.pool import MaybeEncodingError


T = TypeVar('T')
S = TypeVar('S')

logger = logging.getLogger(__name__)


def run_dill_encoded(payload):
    fun, args, idx = dill.loads(payload)
    return idx, fun(*args)


def apply_async_map(pool, fun, args, chunksize: int = 1, timeout=None, callback=None, error_callback=None):
    payloads = [dill.dumps((fun, arg, idx)) for idx, arg in enumerate(args)]
    results = pool.imap_unordered(run_dill_encoded, payloads, chunksize=chunksize)
    unordered_results = []

    while True:
        passed = False
        try:
            result = next(results)
            passed = True
        except StopIteration:
            break
        except Exception as e:
            if error_callback:
                error_callback(e)
            else:
                raise e
        if passed:
            if callback:
                callback(result)
            unordered_results.append(result)

    return [r[1] for r in sorted(unordered_results)]



class AquariumETLABC(ABC):

    @abstractmethod
    def write(self):
        pass

    @abstractmethod
    def read(self):
        pass


class AquariumETL(AquariumETLABC):

    DEBUG_PASSWORD = "debug"

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        # self.__key = Cryptography.generate_key(self.DEBUG_PASSWORD)
        # self.__config = Cryptography.encrypt(self.__key, (uri, user, password))
        self.creds = (uri, user, password)
        try:
            self.setup()
        except ClientError:
            pass

    def f(self, query, **kwargs):
        return format_cypher_query(query, **kwargs)

    def close(self):
        self.driver.close()

    def setup(self):
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT primary_key "
                        "ON (sample:Sample) ASSERT sample.id IS UNIQUE")

    def clear(self):
        with self.driver.session() as sess:
            sess.run("MATCH (a) -[r]-> () DELETE a, r")
            sess.run("MATCH (a) DELETE a")

    def _do_tx(self, query, sess_func, **kwargs):
        def transaction(tx, **data):
            result = tx.run(query, **data)
            return result.values()

        with self.driver.session() as sess:
            return sess_func(sess)(transaction, **kwargs)

    def write(self, query, **kwargs):
        return self._do_tx(query, lambda x: x.write_transaction, **kwargs)

    @staticmethod
    def _write(self, query, **kwargs):
        return self._do_tx(query, lambda x: x.write_transaction, **kwargs)


    def read(self, query, **kwargs):
        return self._do_tx(query, lambda x: x.read_transaction, **kwargs)

    def create(self, model, data=None):
        node_label = model.get_server_model_name()
        data = data or model.dump()
        query = format_cypher_query("""
         CREATE (n:{label})
         SET n.{key} = ${key}
         """, label=node_label, key=list(data))
        return self.write(query, **data)

    def update(self, model):
        node_label = model.get_server_model_name()
        data = model.dump()
        query = format_cypher_query(
            """
            MATCH (n:{label} { id: {id} })
            SET n.{key} = ${key}
            """,
            label=node_label,
            id=str(model.id),
            key=list(data)
        )
        return self.write(query, **data)

    def bind_to_pool(self, n: int):
        """Create a pool of drivers to run a function
        This requires passing credentials many times.


        etl.pool(3)(lambda x: x, args)
        # with etl.pool(10) as pooled:
        #     for query in queries:
        #         pooled.write(query)
        """

    def bind(self, f):
        creds = self.creds
        def bound(*args):
            etl = AquariumETL(*creds)
            return f(etl, *args)
        return bound

    def bind_pool(self, n, f, args: Tuple[Tuple[Any, ...], ...]):
        with Pool(n) as pool:
            results = apply_async_map(pool, self.bind(f), args)
        return results

    def pool(self, n: int):
        """

        Desired API

        etl.pool(4).write(queries)

        :param int:
        :return:
        """
        return PooledAquariumETL(n, self)

class PooledAquariumETL(AquariumETLABC):

    def __init__(self, n, binder: AquariumETL):
        self.binder = binder
        self.n = n

    def __call__(self, func, queries, callback=None, error_callback=None, timeout=None):
        with Pool(self.n) as pool:
            try:
                result = apply_async_map(pool,
                                         self.binder.bind(func),
                                         [(q,) for q in queries],
                                         timeout=timeout,
                                         callback=callback,
                                         error_callback=error_callback, )
            except MaybeEncodingError as e:
                raise e
        return result

    def write(self, queries, callback=None, error_callback=None, timeout=None):
        def _write(etl, query):
            results = etl.write(query)
            return results
        return self(_write, queries, callback=callback, error_callback=error_callback, timeout=timeout)

    def read(self, queries, callback=None, error_callback=None, timeout=None):
        def _read(etl, query):
            results = etl.read(query)
            return results
        return self(_read, queries, callback=callback, error_callback=error_callback, timeout=timeout)