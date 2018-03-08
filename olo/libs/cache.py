from collections import OrderedDict


class LRUCache(object):
    def __init__(self, size=5):
        self.size = size,
        self.cache = OrderedDict()

    def get(self, key):
        if key in self.cache:
            val = self.cache.pop(key)
            self.cache[key] = val
        else:
            val = None

        return val

    def set(self, key, val):
        if key in self.cache:
            val = self.cache.pop(key)
            self.cache[key] = val
        else:
            if len(self.cache) == self.size:
                self.cache.popitem(last=False)
                self.cache[key] = val
            else:
                self.cache[key] = val


class LocalCache(object):
    def __init__(self):
        self._data = {}

    def set(self, key, value):
        self._data[key] = value
        return True

    def get(self, key, default=None):
        return self._data.get(key, default)

    def delete(self, key):
        self._data.pop(key, None)


class IDCache(LocalCache):
    def set(self, value):
        key = id(value)
        super(IDCache, self).set(key, value)
        return key


lc = LocalCache()
ic = IDCache()
