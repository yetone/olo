import types


class N(object):

    def __init__(self, func, default=None, cls=None):
        self.default = default
        self.func = func
        self.cls = cls
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__
        self.__module__ = func.__module__

    def __get__(self, obj, objtype):
        return self.__class__(
            self.func, default=self.default, cls=objtype
        )

    def __call__(self, *args, **kwargs):
        return self.func(self.cls, *args, **kwargs)


def n(default):

    if isinstance(default, classmethod):
        default = default.__func__

    if isinstance(default, types.FunctionType):
        return N(default)

    def _(func):
        return N(func, default=default)

    return _
