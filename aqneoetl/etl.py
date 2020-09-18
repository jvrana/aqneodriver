import logging
from abc import ABC
from abc import abstractmethod
from multiprocessing import Pool
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

import dill
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
from pydent import ModelBase

from .crypto import Cryptography
from .query import format_cypher_query
from aqneoetl.types import ArgsList
from aqneoetl.types import FormatData

T = TypeVar("T")
S = TypeVar("S")

logger = logging.getLogger(__name__)


def run_dill_encoded(payload: bytes) -> Tuple[int, T]:
    fun, args, idx = dill.loads(payload)
    return idx, fun(*args)


def apply_async_map(
    pool: Pool,
    fun: Callable[[T], S],
    args: List[Tuple[T, ...]],
    chunksize: int = 1,
    callback: Optional[Callable[[S], None]] = None,
    error_callback: Optional[Callable[[Exception], None]] = None,
) -> List[S]:
    payloads = [dill.dumps((fun, arg, idx)) for idx, arg in enumerate(args)]
    results = pool.imap_unordered(run_dill_encoded, payloads, chunksize=chunksize)
    unordered_results = []

    while True:
        try:
            result = next(results)
            if callback:
                callback(result)
            unordered_results.append(result)
        except StopIteration:
            break
        except Exception as e:
            if error_callback:
                error_callback(e)
            else:
                raise e

    return [r[1] for r in sorted(unordered_results)]


class AquariumETLABC(ABC):
    @abstractmethod
    def write(self):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def bind(self, f: Callable[["AquariumETL", ArgsList], T]) -> Callable[[S], T]:
        pass


class PooledAquariumETL:
    def __init__(self, n, binder: AquariumETLABC):
        self.binder = binder
        self.n = n

    def __call__(
        self,
        func,
        queries,
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        with Pool(self.n) as pool:
            try:
                result = apply_async_map(
                    pool,
                    self.binder.bind(func),
                    [(q,) for q in queries],
                    callback=callback,
                    error_callback=error_callback,
                )
            except Exception as e:
                # TODO: catch things here
                raise e
        return result

    def write(
        self,
        queries: Union[List[str], Tuple[str, ...]],
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        def _write(etl, query):
            results = etl.write(query)
            return results

        return self(
            _write,
            queries,
            chunksize=chunksize,
            callback=callback,
            error_callback=error_callback,
        )

    def read(
        self,
        queries: Union[List[str], Tuple[str, ...]],
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        def _read(etl, query):
            results = etl.read(query)
            return results

        return self(
            _read,
            queries,
            chunksize=chunksize,
            callback=callback,
            error_callback=error_callback,
        )


class AquariumETL:

    DEBUG_PASSWORD = "debug"

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.__key = Cryptography.generate_key(self.DEBUG_PASSWORD)
        self.__config = Cryptography.encrypt(self.__key, (uri, user, password))
        try:
            self.setup()
        except ClientError:
            pass

    @staticmethod
    def fmt(query: str, **kwargs: FormatData) -> str:

        return format_cypher_query(query, **kwargs)

    def close(self):
        self.driver.close()

    def setup(self):
        with self.driver.session() as session:
            session.run(
                "CREATE CONSTRAINT primary_key "
                "ON (sample:Sample) ASSERT sample.id IS UNIQUE"
            )

    def clear(self):
        with self.driver.session() as sess:
            sess.run("MATCH (a) -[r]-> () DELETE a, r")
            sess.run("MATCH (a) DELETE a")

    def _do_tx(self, query, sess_func, **kwargs: FormatData):
        def transaction(tx, **data):
            result = tx.run(query, **data)
            return result.values()

        with self.driver.session() as sess:
            return sess_func(sess)(transaction, **kwargs)

    def write(self, query: str, **kwargs: FormatData) -> T:
        return self._do_tx(query, lambda x: x.write_transaction, **kwargs)

    @staticmethod
    def _write(self, query, **kwargs: FormatData):
        return self._do_tx(query, lambda x: x.write_transaction, **kwargs)

    def read(self, query: str, **kwargs: FormatData) -> T:
        return self._do_tx(query, lambda x: x.read_transaction, **kwargs)

    def create(self, model: ModelBase, data: FormatData = None) -> T:
        node_label = model.get_server_model_name()
        data = data or model.dump()
        query = format_cypher_query(
            """
         CREATE (n:{label})
         SET n.{key} = ${key}
         """,
            label=node_label,
            key=list(data),
        )
        return self.write(query, **data)

    def update(self, model: ModelBase, data: FormatData = None) -> T:
        node_label = model.get_server_model_name()
        data = data or model.dump()
        query = format_cypher_query(
            """
            MATCH (n:{label} { id: {id} })
            SET n.{key} = ${key}
            """,
            label=node_label,
            id=str(model.id),
            key=list(data),
        )
        return self.write(query, **data)

    def bind(self, f: Callable[["AquariumETL", ArgsList], T]) -> Callable[[S], T]:
        creds = Cryptography.decrypt(self.__key, self.__config, literal_eval=True)

        def bound(*args):
            etl = AquariumETL(*creds)
            return f(etl, *args)

        return bound

    def bind_pool(
        self, n, f: Callable[["AquariumETL", ArgsList], T], args: ArgsList
    ) -> List[T]:
        with Pool(n) as pool:
            results = apply_async_map(pool, self.bind(f), args)
        return results

    def pool(self, n: int) -> PooledAquariumETL:
        """Create a pool of Neo4j sessions.

        etl.pool(4).write(queries)

        :param n: number of processors to use
        :return:
        """
        return PooledAquariumETL(n, self)
