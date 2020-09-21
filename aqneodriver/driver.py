import logging
from abc import ABC
from abc import abstractmethod
from multiprocessing import Pool
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypeVar
from typing import Union

import dill
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
from pydent import ModelBase

from .utils.crypto import Cryptography
from .utils.format_queries import format_cypher_query
from aqneodriver.types import ArgsList
from aqneodriver.types import FormatData
from aqneodriver.types import Payload

T = TypeVar("T")
S = TypeVar("S")

DataType = Dict[
    str,
    Union[
        int,
        float,
    ],
]
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


class AquariumETLDriverABC(ABC):
    @abstractmethod
    def write(self):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def bind(self, f: Callable[["AquariumETLDriver", ArgsList], T]) -> Callable[[S], T]:
        pass


class PooledAquariumETLDriver:
    """Pooled driver that uses multiprocessing to complete tasks.

    Typically instantiated from an :class:`AquariumETLDriver` instances
    as in:

    .. code-block::

        driver.pool(n_jobs=12).write(payloads)

    """
    def __init__(self, n, binder: AquariumETLDriverABC):
        self.binder = binder
        self.n = n

    def __call__(
        self,
        func,
        args: Union[List[Tuple[str, Dict]], Tuple[Tuple[str, Dict], ...]],
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        with Pool(self.n) as pool:
            try:
                result = apply_async_map(
                    pool,
                    self.binder._bind(func),
                    args,
                    callback=callback,
                    error_callback=error_callback,
                )
            except Exception as e:
                # TODO: catch things here
                raise e
        return result

    def read(
        self,
        queries: Union[List[str], Tuple[str, ...], List[Payload], Tuple[Payload, ...]],
        *,
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        def read(
            etl: AquariumETLDriver,
            query: Union[str, Payload],
            data: Dict[str, FormatData],
        ):
            results = etl.read(query, data)
            return results

        return self._bind_func(
            read,
            queries,
            chunksize=chunksize,
            callback=callback,
            error_callback=error_callback,
        )

    def write(
        self,
        queries: Union[List[str], Tuple[str, ...], List[Payload], Tuple[Payload, ...]],
        *,
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        def write(
            etl: AquariumETLDriver,
            query: Union[str, Payload],
            data: Dict[str, FormatData],
        ):
            results = etl.write(query, data)
            return results

        return self._bind_func(
            write,
            queries,
            chunksize=chunksize,
            callback=callback,
            error_callback=error_callback,
        )

    def _bind_func(
        self,
        func: Union[
            str,
            Callable[
                [AquariumETLDriverABC, Union[str, Payload], Dict[str, FormatData]], Any
            ],
        ],
        queries: Union[List[str], Tuple[str, ...], List[Payload], Tuple[Payload, ...]],
        *,
        chunksize: Optional[int] = 1,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ):
        if isinstance(func, str):

            def func(etl: AquariumETLDriver, payload: Payload, data: Dict[FormatData]):
                results = getattr(etl, func)(payload, data)
                return results

        return self(
            func,
            queries,
            chunksize=chunksize,
            callback=callback,
            error_callback=error_callback,
        )


class AquariumETLDriver:
    """The main driver for connecting with Neo4j"""

    DEBUG_PASSWORD = "debug"

    def __init__(self, uri, user, password):
        """Initialize ETL session.

        :param uri:
        :param user:
        :param password:
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.__key = Cryptography.generate_key(self.DEBUG_PASSWORD)
        self.__config = Cryptography.encrypt(self.__key, (uri, user, password))
        try:
            self.setup()
        except ClientError:
            pass

    @staticmethod
    def fmt(query: str, **kwargs: FormatData) -> str:
        """Format cypher query string.

        See :meth:`aqneodriver.query.format_cypher_query
        """
        return format_cypher_query(query, **kwargs)

    def close(self):
        """Close the driver."""
        self.driver.close()

    def setup(self):
        """Add the graphdb constraints.

        :return:
        """
        with self.driver.session() as session:
            session.run(
                "CREATE CONSTRAINT primary_key "
                "ON (sample:Sample) ASSERT sample.id IS UNIQUE"
            )

    # TODO: this should not be easy to run
    def clear(self):
        """
        Clear the graphdb
        :return:
        """
        with self.driver.session() as sess:
            sess.run("MATCH (a) -[r]-> () DELETE a, r")
            sess.run("MATCH (a) DELETE a")

    def _do_tx(
        self,
        query: Union[str, Payload],
        sess_func,
        data: FormatData = None,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = T,
    ) -> Any:
        if isinstance(query, Payload):
            query, pldata = query
            if data:
                pldata.update(data)
            data = dict(pldata)
        else:
            data = dict(data)

        def transaction(tx, **tx_data):
            r = tx.run(query, **tx_data)
            return r.values()

        with self.driver.session() as sess:
            try:
                result = sess_func(sess)(transaction, **data)
            except Exception as e:
                if error_callback:
                    error_callback(e)
                else:
                    raise e
            if callback:
                callback(result)
            return result

    def write(
        self,
        query: Union[str, Payload],
        data: Optional[Dict[str, FormatData]] = None,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ) -> T:
        """Submit a 'write' query to the Neo4j database.

        :param data:
        :param query:
        :param error_callback:
        :param callback:
        :return:
        """
        return self._do_tx(
            query,
            lambda x: x.write_transaction,
            data,
            callback=callback,
            error_callback=error_callback,
        )

    def read(
        self,
        query: Union[str, Payload],
        data: Optional[Dict[str, FormatData]] = None,
        callback: Optional[Callable[[S], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ) -> T:
        """Submit a 'read' query to the Neo4j database.

        :param query:
        :param error_callback:
        :param callback:
        :param data:
        :return:
        """
        return self._do_tx(
            query,
            lambda x: x.read_transaction,
            data,
            callback=callback,
            error_callback=error_callback,
        )

    def aq_create(self, model: ModelBase, data: FormatData = None) -> T:
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
        payload = Payload(query, data)
        return self.write(payload)

    def aq_update(self, model: ModelBase, data: FormatData = None) -> T:
        node_label = model.get_server_model_name()
        data = data or model.dump()
        query = self.fmt(
            """
            MATCH (n:{label} { id: {id} })
            SET n.{key} = ${key}
            """,
            label=node_label,
            id=str(model.id),
            key=list(data),
        )
        payload = Payload(query, data)
        return self.write(payload)

    def _bind(
        self, f: Callable[["AquariumETLDriver", ArgsList], T]
    ) -> Callable[[S], T]:
        """Bind a function to the encrypted credentials of this instance.

        :param f:
        :return:
        """
        creds = Cryptography.decrypt(self.__key, self.__config, literal_eval=True)

        def bound(*args):
            etl = AquariumETLDriver(*creds)
            return f(etl, *args)

        return bound

    def bind_pool(
        self, n, f: Callable[["AquariumETLDriver", ArgsList], T], args: ArgsList
    ) -> List[T]:
        with Pool(n) as pool:
            results = apply_async_map(pool, self._bind(f), args)
        return results

    def pool(self, n: int) -> PooledAquariumETLDriver:
        """Create a pool of Neo4j sessions to run a query with multiple
        processes.

        :param n: number of processors to use
        :return: A pooled ETL interface.
        """
        return PooledAquariumETLDriver(n, self)
