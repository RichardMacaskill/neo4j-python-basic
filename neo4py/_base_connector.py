from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import contextlib
from threading import Lock
from typing import (
    Dict,
    List,
    TYPE_CHECKING
)

import neo4j

from ._common import (
    escape,
    format_escaped,
)

if TYPE_CHECKING:
    from . import Result


class Neo4jConnectorBase:
    def __init__(self, uri, user, password, threads=None):
        self._driver = neo4j.GraphDatabase.driver(
            uri, auth=(user, password)
        )
        # type: Dict[str, List[str]]
        self._bookmarks = defaultdict(list)
        # type: Dict[str, List[str]]
        self._parallel_bookmarks = defaultdict(list)
        # type: Dict[str, Lock]
        self._parallel_bookmarks_lock = Lock()

        self._parallel_mode = False
        self._threads = threads
        self._futures = []
        self.__thread_pool = None

    @property
    def _thread_pool(self):
        if not self.__thread_pool:
            self.__thread_pool = ThreadPoolExecutor(max_workers=self._threads)
        return self.__thread_pool

    @contextlib.contextmanager
    def _get_sequential_session(self, db):
        if self._parallel_mode:
            raise RuntimeError("Cannot execute this operation within "
                               "parallelism block.")
        bookmarks = self._bookmarks[db]
        with self._driver.session(database=db, bookmarks=bookmarks) as session:
            yield session
            self._bookmarks[db] = [session.last_bookmark()]
            try:
                del self._parallel_bookmarks[db]
            except KeyError:
                pass  # nothing to delete here

    @contextlib.contextmanager
    def _get_parallel_session(self, db):
        if not self._parallel_mode:
            raise RuntimeError("Cannot execute this operation outside "
                               "parallelism blocks.")
        bookmarks = self._bookmarks[db]
        with self._driver.session(database=db, bookmarks=bookmarks) as session:
            yield session
            with self._parallel_bookmarks_lock:
                self._parallel_bookmarks[db].append(session.last_bookmark())

    def _query(self, tx_fn, cypher, params):
        from . import Result

        def transaction(tx):
            print(cypher)
            res = tx.run(cypher, params)
            return Result(res.keys(), list(res))
        return tx_fn(transaction)

    def create_node_index(self, label, attributes):
        if isinstance(attributes, str):
            attributes = [attributes]
        else:
            attributes = list(attributes)
        assert attributes
        attributes.sort()
        index_name = "node_index_{}_{}".format(
            label.replace("_", "__"),
            "_".join(attr.replace("_", "__") for attr in attributes)
        )
        self.write_query(format_escaped(
            "CREATE INDEX {} IF NOT EXISTS FOR (n:{}) ON ({{}})",
            index_name, label
        ).format(
            ", ".join("n." + escape(attr) for attr in attributes)
        ))
        return index_name

    def create_rel_index(self, label, attributes):
        if isinstance(attributes, str):
            attributes = [attributes]
        else:
            attributes = list(attributes)
        assert attributes
        attributes.sort()
        index_name = "rel_index_{}_{}".format(
            label.replace("_", "__"),
            "_".join(attr.replace("_", "__") for attr in attributes)
        )
        self.write_query(format_escaped(
            "CREATE INDEX {} IF NOT EXISTS FOR "
            "()-[r:{}]-() ON ({{}})",
            index_name, label
        ).format(
            ", ".join("r." + escape(attr) for attr in attributes)
        ))
        return index_name

    def read_query(self, cypher, params=None, database=None):
        """Execute a cypher read query.

        :param cypher:
        :param params:
        :param database:
        :rtype: Result
        """
        with self._get_sequential_session(database) as session:
            return self._query(session.read_transaction, cypher, params)

    def write_query(self, cypher, params=None, database=None):
        """Execute a cypher write query.

        :param cypher:
        :param params:
        :param database:
        :rtype: Result
        """
        with self._get_sequential_session(database) as session:
            return self._query(session.write_transaction, cypher, params)

    query = write_query

    def parallel_read_query(self, cypher, params=None, database=None):
        def job():
            with self._get_parallel_session(database) as session:
                return self._query(session.read_transaction, cypher,
                                   params)

        future = self._thread_pool.submit(job)
        self._futures.append(future)
        return future

    def parallel_write_query(self, cypher, params=None, database=None):
        def job():
            with self._get_parallel_session(database) as session:
                return self._query(session.write_transaction, cypher,
                                   params)

        future = self._thread_pool.submit(job)
        self._futures.append(future)
        return future

    parallel_query = parallel_write_query

    def _assert_sequential(self):
        if self._parallel_mode:
            raise RuntimeError("Cannot execute this operation within "
                               "parallelism block.")

    def _consolidate_parallel_bookmarks(self):
        for db in self._parallel_bookmarks:
            if self._parallel_bookmarks[db]:
                self._bookmarks[db] = self._parallel_bookmarks[db]
        self._parallel_bookmarks.clear()

    def _await_futures(self):
        for future in self._futures:
            future.result()
        self._futures = []

    @contextlib.contextmanager
    def parallelism(self):
        self._parallel_mode = True
        try:
            yield
        finally:
            self._await_futures()
            self._parallel_mode = False
            self._consolidate_parallel_bookmarks()
