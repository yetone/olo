from olo.compat import PY2


class Key(tuple):
    def __init__(self, *args, **kwargs):
        if PY2:
            super(Key, self).__init__(*args, **kwargs)  # pragma: no cover
        else:
            super().__init__()  # pragma: no cover
        self.hashed_value = self.get_hashed_value()

    def get_hashed_value(self):
        raise NotImplementedError

    def __eq__(self, other):
        if isinstance(other, Key):
            return hash(self) == hash(other)
        return False  # pragma: no cover

    def __ne__(self, other):
        return not self.__eq__(other)  # pragma: no cover

    def __hash__(self):
        return hash(self.hashed_value)


class StrKey(Key):
    def get_hashed_value(self):
        return frozenset(self)

    def __repr__(self):
        return '({})'.format(  # pragma: no cover
            ', '.join(self)
        )
