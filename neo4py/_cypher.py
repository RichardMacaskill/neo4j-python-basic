from ._common import (
    escape,
    escaped_str,
    format_escaped,
    NodeCreationMode,
    RelNodeCreationMode,
    RelCreationMode,
)


class Cypher:
    @staticmethod
    def row_as_dict(attributes, in_name="row", out_name="row",
                    attribute_indexes=None):
        """Create AS statement to convert a row to a dict.

        >>> Cypher.row_as_dict(["foo", "bar"])
        "{`foo`: `row`[0], `bar`: `row`[1]} AS `row`"
        >>> Cypher.row_as_dict(["foo"], in_name="in_row", out_name="out_row")
        "{`foo`: `in_row`[0]} AS `out_row`"
        >>> Cypher.row_as_dict(["foo", "bar"], attribute_indexes=[42, 69])
        "{`foo`: `row`[42], `bar`: `row`[69]} AS `row`"

        :type attributes: Iterable[str]
        :type in_name: str
        :type out_name: str
        :type attribute_indexes: Iterable[int]

        :rtype: str
        """
        if attribute_indexes:
            attr_idx_iter = zip(attribute_indexes, attributes)
        else:
            attr_idx_iter = enumerate(attributes)
        attribute_pairs = (
            format_escaped(f"{{key}}: {escape(in_name)}[{i}]", key=key)
            for i, key in attr_idx_iter
        )
        return "{{{}}} AS {}".format(", ".join(attribute_pairs),
                                     escape(out_name))

    @staticmethod
    def inline_attributes(attributes, row_attr_map=None, row_name="row"):
        """Convert attribute names into a dict depending on row_name.

        >>> Cypher.inline_attributes(["foo", "bar"], row_name="myrow")
        '{`foo`: `myrow`["foo"], bar: `myrow`["bar"]}'
        >>> Cypher.inline_attributes(["fooA"], row_attr_map={"fooA": "foo"})
        '{`foo`: `row`["fooA"]}'

        :type attributes: Iterable[str]
        :type row_attr_map: Dict[str, str] or None
        :type row_name: str

        :rtype: str
        """
        if row_attr_map is None:
            row_attr_map = {}
        if not attributes:
            return ""
        attribute_pairs = [
            "{}: {}[{}]".format(
                escape(row_attr_map.get(key, key)),
                escape(row_name), escaped_str(key)
            )
            for key in attributes
        ]
        return "{{{}}}".format(", ".join(attribute_pairs))

    @staticmethod
    def node_pattern(labels=None, attributes="", name="n"):
        """Create the cypher query needed to create a node pattern.

        :type labels: List[str]
        :type attributes: str
        :type name: str

        :rtype: str
        """
        if labels is None:
            labels = []
        labels = "".join(map(lambda l: ":" + escape(str(l)), labels))
        if attributes and not attributes.startswith(" "):
            attributes = " " + attributes
        return "({}{}{})".format(name, labels, attributes)

    @staticmethod
    def rel_pattern(src_node_pattern, dst_node_pattern, labels, attributes,
                    name="r", directed=True):
        """Create the cypher query needed to create a relationship pattern.

        :type src_node_pattern: str
        :type dst_node_pattern: str
        :type labels: List[str]
        :type attributes: str
        :type name: str
        :type directed: bool

        :rtype: str
        """
        if labels is None:
            labels = []
        labels = "".join(map(lambda l: ":" + escape(str(l)), labels))
        arrow_tip = ">" if directed else ""
        return "{}-[{}{} {}]-{}{}".format(
            src_node_pattern, name, labels, attributes,
            arrow_tip, dst_node_pattern
        )

    @staticmethod
    def nodes_from_rows(attributes, id_, labels, creation_mode, row_attr_map):
        """Create the cypher query needed to create nodes from a list of lists.

        :type attributes: Iterable[str]
        :type id_: str or None
        :type labels: List[str]
        :type creation_mode: NodeCreationMode
        :type row_attr_map: Dict[str, str]

        :rtype: str
        """
        attributes = list(attributes)

        set_statement = "SET n = row"

        if creation_mode == NodeCreationMode.CREATE:
            create_statement = "CREATE {}\n".format(
                Cypher.node_pattern(labels)
            )
        elif creation_mode == NodeCreationMode.MERGE_ON_ID_IGNORE_ON_MATCH:
            create_statement = "MERGE {}\n".format(
                Cypher.node_pattern(
                    labels, Cypher.inline_attributes((id_,), row_attr_map)
                )
            )
            set_statement = "ON CREATE\n  " + set_statement
        elif creation_mode == NodeCreationMode.MERGE_ON_ID_SET_ATTR_ON_MATCH:
            create_statement = "MERGE {}\n".format(
                Cypher.node_pattern(
                    labels, Cypher.inline_attributes((id_,), row_attr_map)
                )
            )
        elif creation_mode == NodeCreationMode.MERGE_ON_ALL_ATTRIBUTES:
            create_statement = "MERGE {}\n".format(
                Cypher.node_pattern(labels,
                                    Cypher.inline_attributes(attributes,
                                                             row_attr_map))
            )
            set_statement = ""
        else:
            raise ValueError("Unknown creation_mode: {}".format(creation_mode))

        with_statement = f"WITH {Cypher.row_as_dict(attributes)}\n"
        return (
            "UNWIND $rows AS row\n"
            + with_statement
            + create_statement
            + set_statement
        )

    @staticmethod
    def rels_from_rows(src_attributes, dst_attributes, rel_attributes,
                       all_attributes,
                       src_labels, dst_labels, rel_labels,
                       src_creation_mode, dst_creation_mode,
                       rel_creation_mode, row_attr_map):
        """Create the cypher query needed to create rels from a list of lists.

        :type src_attributes: Iterable[str]
        :type dst_attributes: Iterable[str]
        :type rel_attributes: Iterable[str]
        :type all_attributes: Iterable[str]
        :type src_labels: Iterable[str]
        :type dst_labels: Iterable[str]
        :type rel_labels: Iterable[str]
        :type src_creation_mode: RelNodeCreationMode
        :type dst_creation_mode: RelNodeCreationMode
        :type rel_creation_mode: RelCreationMode
        :type row_attr_map: Dict[str, str]

        :rtype: str
        """
        src_attributes = list(src_attributes)
        dst_attributes = list(dst_attributes)
        rel_attributes = list(rel_attributes)
        assert all(a in all_attributes for a in src_attributes)
        assert all(a in all_attributes for a in dst_attributes)
        assert all(a in all_attributes for a in rel_attributes)

        src_node_pattern = Cypher.node_pattern(
            src_labels, Cypher.inline_attributes(src_attributes, row_attr_map),
            name="src"
        )
        if src_creation_mode == RelNodeCreationMode.MERGE:
            src_create_statement = f"MERGE {src_node_pattern}\n"
        elif src_creation_mode == RelNodeCreationMode.MATCH:
            src_create_statement = f"MATCH {src_node_pattern}\n"
        elif src_creation_mode == RelNodeCreationMode.CREATE:
            src_create_statement = f"CREATE {src_node_pattern}\n"
        else:
            raise ValueError(
                "Unknown src_creation_mode: {}".format(src_creation_mode)
            )

        dst_node_pattern = Cypher.node_pattern(
            dst_labels, Cypher.inline_attributes(dst_attributes, row_attr_map),
            name="dst"
        )
        if dst_creation_mode == RelNodeCreationMode.MERGE:
            dst_create_statement = f"MERGE {dst_node_pattern}\n"
        elif dst_creation_mode == RelNodeCreationMode.MATCH:
            dst_create_statement = f"MATCH {dst_node_pattern}\n"
        elif dst_creation_mode == RelNodeCreationMode.CREATE:
            dst_create_statement = f"CREATE {dst_node_pattern}\n"
        else:
            raise ValueError(
                "Unknown dst_creation_mode: {}".format(dst_creation_mode)
            )

        rel_pattern = Cypher.rel_pattern(
            Cypher.node_pattern(name="src"),
            Cypher.node_pattern(name="dst"),
            rel_labels, Cypher.inline_attributes(rel_attributes, row_attr_map)
        )
        if rel_creation_mode == RelCreationMode.CREATE:
            rel_create_statement = f"CREATE {rel_pattern}"
        elif rel_creation_mode == RelCreationMode.MERGE:
            rel_create_statement = f"MERGE {rel_pattern}"
        else:
            raise ValueError(
                "Unknown rel_creation_mode: {}".format(rel_creation_mode)
            )

        with_statement = f"WITH {Cypher.row_as_dict(all_attributes)}\n"

        return (
            "UNWIND $rows AS row\n"
            + with_statement
            + src_create_statement
            + dst_create_statement
            + rel_create_statement
        )
