import warnings

from neo4j.graph import (
    Node,
    Relationship,
)

from ._common import (
    format_escaped,
    NodeCreationMode,
    RelCreationMode,
    RelNodeCreationMode,
)
from ._base_connector import Neo4jConnectorBase
from ._cypher import Cypher
from ._base_result import ResultBase


class ResultPandasSupport(ResultBase):
    def to_df(self):
        """Convert any result to a pandas DataFrame.

        ``query("UNWIND range(1, 10) AS n RETURN n, n+1 as m").to_df()``, for
        instance will return a DataFrame with two columns: ``n`` and ``m`` and
        10 rows.

        :rtype: :py:class:`pandas.DataFrame`
        """
        import pandas as pd
        return pd.DataFrame(data={
            k: [r[k] for r in self._records]
            for k in self._keys
        })

    def to_nodes_df(self):
        """Convert a result of nodes to a pandas DataFrame.

        This method assumes that the result contains exactly one node per row.

        The columns of the DataFrame will be the node properties.
        There will be one row per node.

        :rtype: :py:class:`pandas.DataFrame`
        """
        import pandas as pd
        if len(self._keys) != 1:
            raise ValueError("Cannot convert result with != 1 columns to "
                             "node data frame.")
        if not all(isinstance(r[0], Node) for r in self._records):
            raise ValueError("Result needs to contain exactly one Node per "
                             "record row.")
        return pd.DataFrame(data=(dict(r[0]) for r in self._records))

    def to_rel_df(self):
        """Convert a result of relationships to a pandas DataFrame.

        This method assumes that the result contains exactly one relationship
        per row.

        The columns of the DataFrame will be the relationship properties.
        There will be one row per relationship.

        :rtype: :py:class:`pandas.DataFrame`
        """
        import pandas as pd
        if len(self._keys) != 1:
            raise ValueError("Cannot convert result with != 1 columns to "
                             "relationship data frame.")
        if not all(isinstance(r[0], Relationship) for r in self._records):
            raise ValueError("Result needs to contain exactly one "
                             "Relationship per record row.")
        return pd.DataFrame(data=(dict(r[0]) for r in self._records))


class ConnectorPandasSupport(Neo4jConnectorBase):
    def _batch_query_df(self, df, cypher, batch_size, parallel):
        """Run query with batching of DataFrame in parallel or sequentially."""
        # query execution
        if parallel:
            # modes that can be parallelized because queries are independent
            with self.parallelism():
                for offset in range(0, len(df), batch_size):
                    # import time
                    # start = time.perf_counter()
                    rows_list = df[offset:(offset + batch_size)]\
                        .to_numpy().tolist()
                    # end = time.perf_counter()
                    # print(f"Dict transform in {end - start:,} seconds.")
                    self.parallel_write_query(cypher, {"rows": rows_list})
        else:
            # modes that cannot be parallelized because earlier rows might
            # affect later rows.
            for offset in range(0, len(df), batch_size):
                rows_list = df[offset:(offset + batch_size)] \
                    .to_numpy().tolist()
                self.write_query(cypher, {"rows": rows_list})

    def save_nodes_df(self, df, label, id_=None, batch_size=10000,
                      creation_mode=None, df_key_to_attr_map=None,
                      cypher=None):
        """Create nodes in the graph from a `pandas.DataFrame`.

        Each row in the DataFrame represents the attributes of one node.

        :type df: pandas.DataFrame
        :param df: The DataFrame to convert.
        :type label: str
        :param label: the label to apply to the created nodes.
        :type id_: str or None
        :param id_: (optional) the key of the DataFrame column to use as
            primary key. This will create an index on the corresponding
            node attribute, which makes it faster to later create relationships
            between nodes when they are referenced by this attribute.
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
            Defaults to ``CreationMode.CREATE``.
        :type df_key_to_attr_map: dict or None
        :param df_key_to_attr_map: A dictionary mapping the keys of the
            DataFrame to the names of the nodes' attributes in the database.
        :type cypher: str or None
        :param cypher: Overwrite the cypher query which is used to create the
            nodes. You will receive a list of lists in a parameter called
            ``$rows``. The nested list represents the DataFrame without the
            header. Each inner list is one row of the DataFrame.

            Default with ``CreationMode.CREATE`` is:

            UNWIND $rows AS row
            WITH {<df_key0>: row[0], <df_key1>: row[1], ...} AS row
            CREATE (n:<label>)
            SET n = row


            Default with ``CreationMode.MERGE_ON_ID_IGNORE_ON_MATCH`` is:

            UNWIND $rows AS row
            WITH {<df_key0>: row[0], <df_key1>: row[1], ...} AS row
            MERGE (n:<label> {<df_id_key>: row["<df_id_key>"]})
            ON CREATE
              SET n = row


            Default with ``CreationMode.MERGE_ON_ID_SET_ATTR_ON_MATCH`` is:

            UNWIND $rows AS row
            WITH {<df_key0>: row[0], <df_key1>: row[1], ...} AS row
            MERGE (n:<label> {<df_id_key>: row["<df_id_key>"]})
            SET n = row


            Default with ``CreationMode.MERGE_ON_ALL_ATTRIBUTES`` is:

            UNWIND $rows AS row
            WITH {<df_key0>: row[0], <df_key1>: row[1], ...} AS row
            MERGE (n:<label> {<df_key0>: row["<df_key0>"], ...})


            All things in <angle brackets> are derived strings from this
            method's input.

        :rtype: None
        """
        # value normalization
        if creation_mode is None:
            creation_mode = NodeCreationMode.CREATE
        if df_key_to_attr_map is None:
            df_key_to_attr_map = {}

        # type checks
        if not isinstance(label, str):
            raise TypeError(
                "label must be a string, not {}".format(type(label))
            )
        if creation_mode and cypher is not None:
            warnings.warn("Passed label and cypher template to from_nodes_df. "
                          "creation_mode has no effect.")
            creation_mode = NodeCreationMode.CREATE

        if (creation_mode in (NodeCreationMode.MERGE_ON_ID_IGNORE_ON_MATCH,
                              NodeCreationMode.MERGE_ON_ID_SET_ATTR_ON_MATCH)
                and id_ is None):
            raise ValueError("id_ is needed when creation mode is either "
                             "MERGE_ON_ID_IGNORE_ON_MATCH or "
                             "MERGE_ON_ID_SET_ATTR_ON_MATCH")

        # index creation
        if creation_mode == NodeCreationMode.MERGE_ON_ALL_ATTRIBUTES:
            self.create_node_index(label, [df_key_to_attr_map.get(k, k)
                                           for k in df.keys()])
        if id_:
            if id_ not in df:
                raise KeyError("Couldn't find id_ field {} in data frame"
                               .format(id_))
            if creation_mode != NodeCreationMode.MERGE_ON_ALL_ATTRIBUTES:
                self.create_node_index(label, df_key_to_attr_map.get(id_, id_))

        # query generation
        if cypher is None:
            cypher_ = Cypher.nodes_from_rows(df.keys(), id_, [label],
                                             creation_mode, df_key_to_attr_map)
        else:
            cypher_ = cypher

        # query execution
        can_run_parallel = (
            creation_mode in (NodeCreationMode.CREATE,
                              NodeCreationMode.MERGE_ON_ID_IGNORE_ON_MATCH)
            and cypher is None
        )
        self._batch_query_df(df, cypher_, batch_size, can_run_parallel)

    # TODO: add WHERE filer options
    def load_nodes_df(self, label, cypher=None):
        """Retrieve nodes with a given label into a pandas DataFrame.

        :type label: str or List[str]
        :param label: The label used for selecting the nodes.
        :type cypher: str or None
        :param cypher: Overwrite the cypher query which is used to retrieve the
            nodes. Defaults to:

            MATCH (n:<label>) RETURN n

            All things in <angle brackets> are derived strings from this
            method's input.

        :rtype: pandas.DataFrame
        """
        import pandas  # needed in `.to_nodes_df()`, so better fail fast
        if label and cypher:
            warnings.warn("Passed label and cypher template to to_nodes_df. "
                          "Label will be ignored.")
        return self.read_query(
            format_escaped("MATCH (n:{}) RETURN n", label)
        ).to_nodes_df()

    def save_rel_df(self, df, src_label, src_id, dst_label, dst_id, label=None,
                    batch_size=10000, src_attributes=None, dst_attributes=None,
                    src_creation_mode=None, dst_creation_mode=None,
                    creation_mode=None, df_key_to_attr_map=None, cypher=None):
        """Create relationships from a pandas DataFrame.

        :type df: pandas.DataFrame
        :param df: The data frame containing the relationships.
        :type src_label: str
        :param src_label: The label of the source nodes.
        :type src_id: str
        :param src_id: The attribute name of the source nodes to use as id.
            The name in the database must match the column name of the
            DataFrame.
        :type dst_label: str
        :param dst_label: The label of the destination nodes.
        :type dst_id: str
        :param dst_id: The attribute name of the destination nodes to use as
            id.
            The name in the database must match the column name of the
            DataFrame.
        :type label: str or List[str] or None
        :param label: The label of the relationships.
        :type batch_size: int
        :param batch_size: The number of relationships to write in one batch.
        :type src_attributes: List[str] or None
        :param src_attributes: The keys of the DataFrame used as attributes of
            the source nodes.
        :type dst_attributes: List[str] or None
        :param dst_attributes: The keys of the DataFrame used as attributes of
            the destination nodes.
        :type src_creation_mode: RelNodeCreationMode or None
        :param src_creation_mode: The mode used to create the source nodes.
            ``RelNodeCreationMode.MERGE``:
                Use ``MERGE`` instruction to match or create the source node
                according to the ``src_id`` and ``src_attributes``.
            ``RelNodeCreationMode.MATCH``:
                Use ``MATCH`` instruction to only match the src node according
                to ``src_id`` and ``src_attributes``. If a matching node does
                not exist, the row is skipped.
            ``RelNodeCreationMode.CREATE``:
                 Always create a new src node with ``src_id`` and
                 ``src_attributes``.

            Defaults to ``RelNodeCreationMode.MERGE``.
        :type dst_creation_mode: RelNodeCreationMode or None
        :param dst_creation_mode: Same as ``src_creation_mode``, but for the
            destination nodes.
        :type creation_mode: RelCreationMode or None
        :param creation_mode: Change the way nodes are created.
            ``RelCreationMode.CREATE``:
                Always create a new relationship for each row (might introduce
                duplicates). Fastest operation.
            ``RelCreationMode.MERGE``:
                Use ``MERGE`` instruction and match on the whole row. If a
                relationship with exactly the same attributes already exists,
                between src and dst, the row is skipped.

            For more details see the ``cypher`` parameter.
            Defaults to ``RelCreationMode.CREATE``.
        :type df_key_to_attr_map: dict or None
        :param df_key_to_attr_map: A dictionary mapping the keys of the
            DataFrame to the names of the nodes' and relationships' attributes
            in the database.
        :type cypher: str or None
        :param cypher: Overwrite the cypher query which is used to create the
            relationships.

            You will receive a list of lists in a parameter called
            ``$rows``. The nested list represents the DataFrame without the
            header. Each inner list is one row of the DataFrame.

            Defaults to:

            UNWIND $rows AS row
            WITH {<df_key0>: row[0], <df_key1>: row[1], ...} AS row
            MERGE (src:<src_label> {<df_src_id>: row["<df_src_id>"]})
            MERGE (dst:<dst_label> {<df_dst_id>: row["<df_dst_id>"]})
            CREATE (src)-[r:<label> {<df_key0>: row["<df_key0>"], ...}]->(dst)
            SET n = row

            You can alter ``MERGE`` for ``(src:...)`` with the parameter
            ``src_creation_mode``.
            Likewise, you can alter ``MERGE`` for ``(dst:...)`` with the
            parameter ``dst_creation_mode``.
            If you want further control over the attributes of ``src`` or
            ``dst`` (i.e. more attributes than the id), you can use the
            parameters ``src_attributes`` and ``dst_attributes``.

            Use ``creation_mode`` to alter the ``CREATE`` in front of the
            relationship. ``df_key0`` to ``df_keyN`` will be all columns of the
            DataFrame except the ids and attributes used for ``src`` and
            ``dst``.

            All things in <angle brackets> are derived strings from this
            method's input.

        :rtype: None
        """
        # value normalization
        if creation_mode is None:
            creation_mode = RelCreationMode.CREATE
        if src_creation_mode is None:
            src_creation_mode = RelNodeCreationMode.MERGE
        if dst_creation_mode is None:
            dst_creation_mode = RelNodeCreationMode.MERGE
        if src_attributes is None:
            src_attributes = []
        if dst_attributes is None:
            dst_attributes = []
        if df_key_to_attr_map is None:
            df_key_to_attr_map = {}
        non_rel_attributes = {src_id, dst_id, *src_attributes, *dst_attributes}
        rel_attributes = [a for a in df.keys() if a not in non_rel_attributes]

        # type checks
        if not isinstance(label, str):
            raise TypeError(
                "label must be a string, not {}".format(type(label))
            )

        for name in ("src_label", "dst_label", "src_id", "dst_id"):
            val = locals()[name]
            if not isinstance(val, str):
                raise TypeError(
                    "{} must be a string, not {}".format(name, type(val))
                )

        if src_id not in df:
            raise KeyError("Couldn't find src_id field {} in data frame"
                           .format(src_id))
        if dst_id not in df:
            raise KeyError("Couldn't find dst_id field {} in data frame"
                           .format(dst_id))

        src_attributes = [src_id, *src_attributes]
        dst_attributes = [dst_id, *dst_attributes]

        # index creation
        self.create_node_index(src_label, [df_key_to_attr_map.get(k, k)
                                           for k in src_attributes])
        self.create_node_index(dst_label, [df_key_to_attr_map.get(k, k)
                                           for k in dst_attributes])
        self.create_rel_index(label, [df_key_to_attr_map.get(k, k)
                                      for k in rel_attributes])

        # query generation
        if cypher is None:
            cypher_ = Cypher.rels_from_rows(
                src_attributes, dst_attributes, rel_attributes, df.keys(),
                [src_label], [dst_label], [label],
                src_creation_mode, dst_creation_mode, creation_mode,
                df_key_to_attr_map
            )
        else:
            cypher_ = cypher

        # query execution
        can_run_parallel = (
            creation_mode == RelCreationMode.CREATE
            and src_creation_mode in (
                RelNodeCreationMode.MATCH, RelCreationMode.CREATE
            )
            and dst_creation_mode in (
                RelNodeCreationMode.MATCH, RelCreationMode.CREATE
            )
        )
        self._batch_query_df(df, cypher_, batch_size, can_run_parallel)

    # TODO: add WHERE filer options
    def load_rel_df(self, label, cypher=None):
        """Retrieve relationships with a given label into a pandas DataFrame.

        :type label: str or List[str]
        :param label: The label(s) used for selecting the nodes.
        ;type rel_attributes: List[str]
        :type cypher: str or None
        :param cypher: Overwrite the cypher query which is used to retrieve the
            nodes. Defaults to:

            MATCH ()-[r:<label>]->() RETURN r

            All things in <angle brackets> are derived strings from this
            method's input.

        :rtype: pandas.DataFrame
        """
        import pandas  # needed in `.to_nodes_df()`, so better fail fast
        if label and cypher:
            warnings.warn("Passed label and cypher template to to_nodes_df. "
                          "Label will be ignored.")
        return self.read_query(
            format_escaped("MATCH ()-[r:{}]->() RETURN r", label)
        ).to_rel_df()
