import warnings


class UnsupportedExtra(ImportError):
    def __init__(self, extra_name):
        warnings.warn("The desired operation requires {} to be installed."
                      .format(extra_name))
        super().__init__("This operation requires {} as dependency."
                         .format(extra_name))
