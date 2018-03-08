CACHED_DECLARED_ATTR_NAME = '_olo_declared_attr_cached'
missing = object()


class declared_attr(object):
    def __init__(self, fget):
        self.fget = fget
        self.__name__ = fget.__name__
        self.__doc__ = fget.__doc__

    def __get__(self, obj, objtype):
        cached = getattr(objtype, CACHED_DECLARED_ATTR_NAME, None)
        if cached is None:
            cached = {}
            setattr(objtype, CACHED_DECLARED_ATTR_NAME, cached)
        v = cached.get(self.__name__, missing)
        if v is missing:
            cached[self.__name__] = v = self.fget(objtype)
        if hasattr(v, '__get__'):
            return v.__get__(obj, objtype)
        return v
