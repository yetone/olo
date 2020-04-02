import weakref
from functools import wraps
from typing import TypeVar, Generic

from olo.types.json import JSONLike

T = TypeVar('T')


class OrmType(Generic[T]):
    value: T

    def __init__(self, value: T) -> None:
        self.value = value


class Json(OrmType[JSONLike]):
    pass


class TrackedValue(object):
    def __init__(self, obj, attr):
        self.obj_ref = weakref.ref(obj)
        self.attr = attr

    @classmethod
    def make(cls, obj, attr, value):
        if isinstance(value, dict):
            return TrackedDict(obj, attr, value)
        if isinstance(value, list):
            return TrackedList(obj, attr, value)
        return value

    def _changed_(self):
        obj = self.obj_ref()
        if obj is not None:
            obj._dirty_fields.add(self.attr)

    def get_untracked(self):
        assert False, 'Abstract method'  # pragma: no cover


def tracked_method(func):
    @wraps(func)
    def new_func(self, *args, **kwargs):
        obj = self.obj_ref()
        attr = self.attr
        if obj is not None:
            args = tuple(TrackedValue.make(obj, attr, arg) for arg in args)
            if kwargs:
                kwargs = {key: TrackedValue.make(obj, attr, value) for key, value in kwargs.items()}
        result = func(self, *args, **kwargs)
        self._changed_()
        return result
    return new_func


class TrackedDict(TrackedValue, dict):
    def __init__(self, obj, attr, value):
        TrackedValue.__init__(self, obj, attr)
        dict.__init__(self, {key: self.make(obj, attr, val) for key, val in value.items()})

    def __reduce__(self):
        return dict, (dict(self),)

    __setitem__ = tracked_method(dict.__setitem__)
    __delitem__ = tracked_method(dict.__delitem__)
    _update = tracked_method(dict.update)

    def update(self, *args, **kwargs):
        args = [arg if isinstance(arg, dict) else dict(arg) for arg in args]
        return self._update(*args, **kwargs)

    setdefault = tracked_method(dict.setdefault)
    pop = tracked_method(dict.pop)
    popitem = tracked_method(dict.popitem)
    clear = tracked_method(dict.clear)

    def get_untracked(self):
        return {key: val.get_untracked() if isinstance(val, TrackedValue) else val
                for key, val in self.items()}


class TrackedList(TrackedValue, list):
    def __init__(self, obj, attr, value):
        TrackedValue.__init__(self, obj, attr)
        list.__init__(self, (self.make(obj, attr, val) for val in value))

    def __reduce__(self):
        return list, (list(self),)

    __setitem__ = tracked_method(list.__setitem__)
    __delitem__ = tracked_method(list.__delitem__)
    extend = tracked_method(list.extend)
    append = tracked_method(list.append)
    pop = tracked_method(list.pop)
    remove = tracked_method(list.remove)
    insert = tracked_method(list.insert)
    reverse = tracked_method(list.reverse)
    sort = tracked_method(list.sort)
    clear = tracked_method(list.clear)

    def get_untracked(self):
        return [val.get_untracked() if isinstance(val, TrackedValue) else val for val in self]
