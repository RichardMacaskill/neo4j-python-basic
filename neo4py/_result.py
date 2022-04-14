from ._numpy_support import ResultNumpySupport
from ._pandas_support import ResultPandasSupport


class Result(ResultNumpySupport,
             ResultPandasSupport):
    pass
