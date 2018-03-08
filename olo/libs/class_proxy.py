class ClassProxy(object):
    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, item):
        return getattr(self._raw, item)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __nonzero__(self):
        return bool(self._raw)

    __bool__ = __nonzero__
