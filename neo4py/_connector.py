from ._numpy_support import ConnectorNumpySupport
from ._pandas_support import ConnectorPandasSupport


class Neo4jConnector(ConnectorNumpySupport,
                     ConnectorPandasSupport):
    pass
