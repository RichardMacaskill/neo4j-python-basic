from enum import Enum


class NodeCreationMode(Enum):
    CREATE = 1
    MERGE_ON_ID_IGNORE_ON_MATCH = 2
    MERGE_ON_ID_SET_ATTR_ON_MATCH = 3
    MERGE_ON_ALL_ATTRIBUTES = 4


class RelNodeCreationMode(Enum):
    MERGE = 1
    MATCH = 2
    CREATE = 3


class RelCreationMode(Enum):
    CREATE = 1
    MERGE = 2


def escape(str_):
    return "`" + str_.replace("`", "``") + "`"


def format_escaped(str_, *args, **kwargs):
    return str_.format(*map(escape, args),
                       **{k: escape(v) for k, v in kwargs.items()})


def escaped_str(str_):
    return '"' + str_.replace("\\", r"\\").replace('"', r'\"') + '"'


def format_escaped_str(str_, *args, **kwargs):
    return str_.format(*map(escaped_str, args),
                       **{k: escaped_str(v) for k, v in kwargs.items()})
