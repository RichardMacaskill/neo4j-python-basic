import collections.abc
import warnings

import numpy as np

from neo4j.graph import Node

from ._base_connector import Neo4jConnectorBase
from ._common import (
    NodeCreationMode,
    format_escaped,
)
from ._cypher import (
    Cypher,
)
from ._base_result import ResultBase


class ResultNumpySupport(ResultBase):
    def to_np(self, dtype=None):
        """Convert any result to a NumPy ndarray.

        ``query("UNWIND range(1, 10) AS n RETURN n, n+1 as m").to_df()``, for
        instance will return a 10x2 array each row represents on n, m pair,
        the columns contain n and m.
        In fact, this method returns a tuple. The first element is the list
        of column names ("n" and "m" in this case), the second element is the
        NumPy array.

        :param dtype: The NumPy dtype to use for the array.
        :rtype: Tuple[List[str], `py:class:`numpy.ndarray`]
        """
        import numpy as np
        if dtype is not None:
            dtype = np.dtype(dtype)
        if (dtype is np.dtype(object)
                or (any(isinstance(e, collections.abc.Mapping)
                    for rec in self._records for e in rec))):
            out = np.empty((len(self._records), len(self._keys)),
                           dtype=object)
            out[:] = self._records
            return tuple(self._keys), out

        if dtype is None:
            return tuple(self._keys), np.array(list(map(list, self._records)))
        else:
            shape = []
            dim = self._records
            while hasattr(dim, "__len__"):
                shape.append(len(dim))
                dim = dim[0]
            out = np.empty(shape, dtype=dtype)
            out[:] = self._records
            return tuple(self._keys), out

    def to_nodes_np(self):
        """Convert a result of nodes to a NumPy ndarray.

        This method assumes that the result contains exactly one node per row.

        Each row is a node, and the columns contain the properties of the node.
        In fact, this method returns a tuple. The first element is the list
        of property names, the second element is the NumPy array.

        :rtype: :py:class:`pandas.DataFrame`
        :rtype: Tuple[List[str], `py:class:`numpy.ndarray`]
        """
        import numpy as np
        if len(self._keys) != 1:
            raise ValueError("Cannot convert result with != 1 columns to "
                             "data frame.")
        if not all(isinstance(r[0], Node) for r in self._records):
            raise ValueError("Result needs to contain exactly one Node per "
                             "record row.")
        return (tuple(self._records[0][0].keys()),
                np.array([list(r[0].values()) for r in self._records]))


class ConnectorNumpySupport(Neo4jConnectorBase):
    def save_nodes_np(self, array, attributes, label, id_=None,
                      batch_size=10000, creation_mode=None, cypher=None):
        """Create nodes in the graph from a `np.ndarray`.

        Each row in the array represents the attributes of one node.

        :type array: np.ndarray
        :param array: The numpy array to convert.
        :type attributes: List[str]
        :param attributes: Map the columns of the array to attribute names of
            the nodes.
        :type label: str
        :param label: the label to apply to the created nodes.
        :param id_: (optional) the attribute to use as primary key. This will
            create an index on the corresponding node attribute, which makes it
            faster to later create relationships between nodes when they are
            referenced by this attribute.
        :type batch_size: int
        :param batch_size: The nodes will be created in batches. Choose the
            batch size.
        :type creation_mode: NodeCreationMode or None
        :param creation_mode: Change the way nodes are created.
            ``CreationMode.CREATE``:
                Always create a new node for each row (might introduce
                duplicates). Fastest operation.
            ``CreationMode.MERGE_ON_ID_IGNORE_ON_MATCH``:
                Use ``MERGE`` instruction and match on the specified ``id_``
                key. If a node with the same ``id_`` already exists, this row
                is skipped.
            ``CreationMode.MERGE_ON_ID_SET_ATTR_ON_MATCH``:
                Like ``CreationMode.MERGE_ON_ID_IGNORE_ON_MATCH``, but will
                update the nodes attributes if a node with the same ``id_``
                already exists.
            ``CreationMode.MERGE_ON_ALL_ATTRIBUTES``:
                Use ``MERGE`` instruction and match on the whole row. If a node
                with exactly the same attributes already exists, the row is
                skipped.

            For more details see the ``cypher`` parameter.
            Defaults to: ``CreationMode.CREATE``.
        :type cypher: str or None
        :param cypher: Overwrite the cypher query which is used to create the
            nodes. You will receive a list of lists in a parameter called
            ``$rows``. The nested list represents the array without the
            header. Each inner list is one row of the array.

            Default with ``CreationMode.CREATE`` is:

            UNWIND $rows AS row
            WITH {<attributes_0>: row[0], <attributes_1>: row[1], ...} AS row
            CREATE (n:<label_1>:<label_2>...)
            SET n = row


            Default with ``CreationMode.MERGE_ON_ID_IGNORE_ON_MATCH`` is:

            UNWIND $rows AS row
            WITH {<attributes_0>: row[0], <attributes_1>: row[1], ...} AS row
            MERGE (n:<label_1>... {<df_id_key>: row["<df_id_key>"]})
            ON CREATE
              SET n = row


            Default with ``CreationMode.MERGE_ON_ID_SET_ATTR_ON_MATCH`` is:

            UNWIND $rows AS row
            WITH {<attributes_0>: row[0], <attributes_1>: row[1], ...} AS row
            MERGE (n:<label_1>... {<df_id_key>: row["<df_id_key>"]})
            SET n = row


            Default with ``CreationMode.MERGE_ON_ALL_ATTRIBUTES`` is:

            UNWIND $rows AS row
            WITH {<attributes_0>: row[0], <attributes_1>: row[1], ...} AS row
            MERGE (n:<label_1>... {<attributes_0>: row["<attributes_0>"], ...})


            All things in <angle brackets> are derived strings from this
            method's input.

        :rtype: None
        """
        # value normalization
        if creation_mode is None:
            creation_mode = NodeCreationMode.CREATE

        # type checks
        if not isinstance(label, str):
            raise TypeError(
                "label must be a string, not {}".format(type(label))
            )
        if (not cypher
            and (not isinstance(attributes, (list, tuple))
                 or not all(isinstance(attr, str) for attr in attributes))):
            raise TypeError("attributes must be a list of strings")
        if len(array.shape) < 2 or array.shape[1] != len(attributes):
            raise ValueError("array must have shape (n, m[, ...]) with m >= 2 "
                             "and m == len(attributes), but had shape {}"
                             .format(array.shape))
        if (creation_mode in (NodeCreationMode.MERGE_ON_ID_IGNORE_ON_MATCH,
                              NodeCreationMode.MERGE_ON_ID_SET_ATTR_ON_MATCH)
                and id_ is None):
            raise ValueError("id_ is needed when creation mode is either "
                             "MERGE_ON_ID_IGNORE_ON_MATCH or "
                             "MERGE_ON_ID_SET_ATTR_ON_MATCH")

        # index creation
        if creation_mode == NodeCreationMode.MERGE_ON_ALL_ATTRIBUTES:
            self.create_node_index(label, attributes)
        if id_:
            if id_ not in attributes:
                raise ValueError("id_ must be in attributes")
            if creation_mode != NodeCreationMode.MERGE_ON_ALL_ATTRIBUTES:
                self.create_node_index(label, id_)

        if cypher and (creation_mode or attributes):
            warnings.warn("Passed label and cypher template to from_nodes_df. "
                          "Label will be ignored.")

        # query generation
        if cypher is None:
            cypher_ = Cypher.nodes_from_rows(attributes, id_, [label],
                                             creation_mode)
        else:
            cypher_ = cypher

        # query execution
        if creation_mode in (NodeCreationMode.CREATE,
                             NodeCreationMode.MERGE_ON_ID_IGNORE_ON_MATCH):
            # modes that can be parallelized because queries are independent
            with self.parallelism():
                for offset in range(0, len(array), batch_size):
                    # import time
                    # start = time.perf_counter()
                    rows_list = array[offset:(offset + batch_size)].tolist()
                    # end = time.perf_counter()
                    # print(f"Dict transform in {end - start:,} seconds.")
                    self.parallel_write_query(cypher_, {"rows": rows_list})
        else:
            # modes that cannot be parallelized because earlier rows might
            # affect later rows.
            for offset in range(0, len(array), batch_size):
                rows_list = array[offset:(offset + batch_size)].tolist()
                self.write_query(cypher_, {"rows": rows_list})

    # TODO: add WHERE filer options
    def load_nodes_np(self, label, cypher=None):
        """Retrieve nodes with (a) given label(s) into a numpy array.

        :type label: str or List[str]
        :param label: The label(s) used for selecting the nodes.
        :type cypher: str or None
        :param cypher: Overwrite the cypher query which is used to retrieve the
            nodes. Defaults to:

            MATCH (n:<label_1>:<label_2>...) RETURN n

            All things in <angle brackets> are derived strings from this
            method's input.

        :rtype: numpy.ndarray
        """
        import pandas  # needed in `.to_nodes_df()`, so better fail fast
        if label and cypher:
            warnings.warn("Passed label and cypher template to to_nodes_df. "
                          "Label will be ignored.")
        return self.read_query(
            format_escaped("MATCH (n:{}) RETURN n", label)
        ).to_nodes_np()
