# pylint: disable=undefined-variable,used-before-assignment

try:
    from cdecimal import Decimal
except ImportError:
    from decimal import Decimal  # noqa

import sys

PY2 = sys.version_info[0] == 2

if PY2:
    from future_builtins import zip as izip, map as imap
    import __builtin__ as builtins
    import cPickle as pickle
    from cStringIO import StringIO
    from Queue import Queue, LifoQueue, Empty

    xrange = xrange
    basestring = basestring
    unicode = unicode
    buffer = buffer
    int_types = (int, long)
    str_types = (basestring,)
    cmp = cmp
    long = long
    reduce = reduce

    def iteritems(dict):
        return dict.iteritems()  # pragma: no cover

    def iterkeys(dict):
        return dict.iterkeys()

    def itervalues(dict):
        return dict.itervalues()  # pragma: no cover

    def items_list(dict):
        return dict.items()

    def values_list(dict):
        return dict.values()  # pragma: no cover

    def to_str(x, charset='utf8', errors='strict'):
        if x is None or isinstance(x, str):
            return x

        if isinstance(x, unicode):
            return x.encode(charset, errors)

        return str(x)

else:
    import builtins  # noqa pragma: no cover
    import pickle  # noqa pragma: no cover
    from io import StringIO  # noqa pragma: no cover
    from queue import Queue, LifoQueue, Empty
    from functools import reduce  # noqa pragma: no cover

    izip, imap, xrange = zip, map, range  # pragma: no cover
    basestring = str  # pragma: no cover
    unicode = str  # pragma: no cover
    buffer = bytes  # pragma: no cover
    int_types = (int,)  # pragma: no cover
    str_types = (str,)
    long = int  # pragma: no cover

    def cmp(a, b):  # pragma: no cover
        return (a > b) - (a < b)  # pragma: no cover

    def iteritems(dict):  # pragma: no cover
        return iter(dict.items())  # pragma: no cover

    def iterkeys(dict):  # pragma: no cover
        return iter(dict.keys())  # pragma: no cover

    def itervalues(dict):  # pragma: no cover
        return iter(dict.values())  # pragma: no cover

    def items_list(dict):  # pragma: no cover
        return list(dict.items())  # pragma: no cover

    def values_list(dict):  # pragma: no cover
        return list(dict.values())  # pragma: no cover

    def to_str(x, charset='utf8', errors='strict'):
        if x is None or isinstance(x, str):
            return x

        if isinstance(x, bytes):
            return x.decode(charset, errors)

        return str(x)


# noqa Armin's recipe from http://lucumr.pocoo.org/2013/5/21/porting-to-python-3-redux/
def with_metaclass(meta, *bases):
    class metaclass(meta):
        __call__ = type.__call__
        __init__ = type.__init__

        def __new__(cls, name, this_bases, d):
            if this_bases is None:
                return type.__new__(cls, name, (), d)
            return meta(name, bases, d)
    return metaclass('temporary_class', None, {})
