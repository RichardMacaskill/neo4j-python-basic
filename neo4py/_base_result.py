import neo4j


class ResultBase:
    def __init__(self, keys, records):
        self._keys = keys
        self._records = records

    @property
    def keys(self):
        return self._keys

    @property
    def records(self):
        return self._records

    def select(self, *keys):
        return self.__class__(
            keys,
            [neo4j.Record(zip(keys, r.values(*keys))) for r in self._records]
        )
