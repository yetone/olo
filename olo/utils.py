from __future__ import absolute_import

import inspect
import json
import re
import threading
from enum import Enum
from typeguard import check_type

import dateparser
import logging
from string import Formatter
from warnings import warn
from functools import wraps
from ast import literal_eval
from datetime import datetime, date
from collections import OrderedDict

from olo.compat import (
    PY2, Decimal, unicode, iteritems, str_types, get_items,
    izip,
)
from olo.types.json import JSONLike


def camel2underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def to_camel_case(snake_str):
    pieces = snake_str.split('_')
    return pieces[0] + ''.join(x.title() for x in pieces[1:])


class Missing(object):

    def __eq__(self, other):
        return isinstance(other, self.__class__)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __nonzero__(self):
        return False

    __bool__ = __nonzero__


missing = Missing()


def deprecation(msg):
    warn(msg, DeprecationWarning, stacklevel=2)


keywords = {
    'select',
    'insert',
    'update',
    'into',
    'values',
    'from',
    'and',
    'or',
    'null',
    'group by',
    'order by',
    'desc',
    'asc',
    'limit',
    'offset',
    'where',
    'in',
    'sum',
    'count',
    'avg',
    'max',
    'min',
    'group_concat',
    'as',
}


def type_checker(type_, obj):  # pylint: disable=too-many-return-statements
    if type_ is JSONLike:
        return True
    if isinstance(type_, type) and isinstance(obj, type_):
        return True
    try:
        from typing import List
        if isinstance(type_, List.__class__):
            check_type('', obj, type_)
            return True
    except (TypeError, ImportError):
        pass
    t = type(type_)
    if t != type(obj):
        return False
    if t is list:
        if not type_:
            return isinstance(obj, t)
        _t = type_[0]
        for e in obj:
            r = type_checker(_t, e)
            if not r:
                return False
        return True
    if t is tuple:
        if len(type_) != len(obj):
            return False
        for i, e in enumerate(obj):
            r = type_checker(type_[i], e)
            if not r:
                return False
        return True
    if t is dict:
        items = get_items(type_)
        if not items:
            return isinstance(obj, t)
        kt, vt = items[0]
        for k, v in iteritems(obj):
            if not type_checker(kt, k) or not type_checker(vt, v):
                return False
        return True
    return False


def transform_type(obj, type_):  # pylint: disable=too-many-return-statements
    if type_ is JSONLike:
        return obj
    if isinstance(type_, type) and isinstance(obj, type_):
        return obj
    if isinstance(type_, type) and issubclass(type_, Enum):
        if obj not in type_.__members__:
            raise TypeError(f'{obj} is not a valid type of enum {type_}')
        return getattr(type_, obj)
    if type_ is str:
        if isinstance(obj, unicode):
            return obj.encode('utf-8')  # pragma: no cover
        if isinstance(obj, (list, dict)):
            return json.dumps(obj)
        if isinstance(obj, Enum):
            return obj.name
        return type_(obj)
    if type_ is unicode:
        if isinstance(obj, str):  # pragma: no cover
            return obj.decode('utf-8')  # pragma: no cover
        return type_(obj)  # pragma: no cover
    if type_ in (list, dict):
        if isinstance(obj, str_types):
            obj = json.loads(obj)
            if isinstance(obj, type_):
                return obj
        return type_(obj)
    if type_ in (datetime, date):
        obj = dateparser.parse(obj)
        if type_ is date:
            return obj.date()
        return obj
    if type_ is tuple:
        if isinstance(obj, str_types):
            obj = literal_eval(obj)
            if isinstance(obj, type_):
                return obj
        return tuple(obj)
    if callable(type_):
        if type_ is Decimal:
            return type_(str(obj))
        return type_(obj)
    t = type(type_)
    if t in (list, dict) and isinstance(obj, str_types):
        obj = json.loads(obj)
    if not isinstance(obj, t):
        raise TypeError('{} is not a {} type.'.format(repr(obj), t))
    if isinstance(obj, list):
        return [transform_type(e, type_[0]) for e in obj]
    if isinstance(obj, dict):
        d = {}
        items = get_items(type_)
        kt, vt = items[0]
        for k, v in iteritems(obj):
            k = transform_type(k, kt)
            v = transform_type(v, vt)
            d[k] = v
        return d
    return obj


class ThreadedObject(object):
    def __init__(self, cls, *args, **kw):
        self.local = threading.local()
        self._args = (cls, args, kw)

        def creator():
            return cls(*args, **kw)

        self.creator = creator

    def __getstate__(self):
        return self._args

    def __setstate__(self, state):
        cls, args, kw = state
        self.__init__(cls, *args, **kw)

    def __getattr__(self, name):
        obj = getattr(self.local, 'obj', None)
        if obj is None:
            self.local.obj = obj = self.creator()
        return getattr(obj, name)


class cached_property(object):

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __set__(self, obj, value):
        obj.__dict__[self.__name__] = value

    def __get__(self, obj, type=None):
        if obj is None:
            return self  # pragma: no cover
        value = obj.__dict__.get(self.__name__, missing)
        if value is missing:
            value = self.func(obj)
            obj.__dict__[self.__name__] = value
        return value


def readonly_cached_property(func):
    attr_name = '_%s' % func.__name__

    @property
    @wraps(func)
    def _(self):
        if attr_name not in self.__dict__:
            setattr(self, attr_name, func(self))
        return self.__dict__[attr_name]

    return _


def override(func):
    setattr(func, '_override', True)
    return func


_OPERATOR_PRECEDENCES = (
    ('*', '/', '%', 'DIV', 'MOD'),
    ('-', '+'),
    ('<<', '>>'),
    ('=', '!=', '>', '<', '>=', '<=', 'IN', 'IS', 'IS NOT', 'NOT IN'),
    ('BETWEEN', 'CASE'),
    ('AND', '&&'),
    ('OR', '||'),
)


OPERATOR_PRECEDENCES = {
    item: idx
    for idx, items in enumerate(reversed(_OPERATOR_PRECEDENCES))
    for item in items
}


UNARY_NEG_OPERATOR = {
    '-': '+'
}


UNARY_NEG_OPERATOR = dict({
    v: k
    for k, v in iteritems(UNARY_NEG_OPERATOR)
}, **UNARY_NEG_OPERATOR)


BINARY_NEG_OPERATOR = {
    'IN': 'NOT IN',
    'IS': 'IS NOT',
    '=': '!=',
    '>': '<=',
    '<': '>='
}


BINARY_NEG_OPERATOR = dict({
    v: k
    for k, v in iteritems(BINARY_NEG_OPERATOR)
}, **BINARY_NEG_OPERATOR)


def get_neg_operator(op, is_unary=False):
    op = op.strip().upper()
    if is_unary:
        return UNARY_NEG_OPERATOR.get(op)  # pragma: no cover
    return BINARY_NEG_OPERATOR.get(op)


def get_operator_precedence(operator):
    return OPERATOR_PRECEDENCES.get(operator, -1)


def compare_operator_precedence(a, b):
    ap = get_operator_precedence(a.upper())
    bp = get_operator_precedence(b.upper())
    if ap == bp:
        return 0  # pragma: no cover
    if ap > bp:
        return 1
    return -1  # pragma: no cover


def friendly_repr(v):
    if not PY2:
        return repr(v)  # pragma: no cover
    if isinstance(v, unicode):  # pragma: no cover
        return "u'%s'" % v.encode('utf-8')  # pragma: no cover
    if isinstance(v, bytes):  # pragma: no cover
        return "b'%s'" % v  # pragma: no cover
    return repr(v)  # pragma: no cover


def is_under_thread():
    return threading.current_thread().name != 'MainThread'  # pragma: no cover


def make_thread_safe_class(base, method_names=()):
    __class__ = type('', (base,), {})

    def __init__(self, *args, **kwargs):
        super(__class__, self).__init__(*args, **kwargs)  # pragma: no cover
        self.lock = threading.RLock()  # pragma: no cover

    __class__.__init__ = __init__

    def make_method(name):
        def method(self, *args, **kwargs):
            with self.lock:  # pragma: no cover
                return getattr(super(__class__, self), name)(*args, **kwargs)  # noqa pragma: no cover
        method.__name__ = name
        return method

    for name in method_names:
        setattr(__class__, name, make_method(name))

    return __class__


ThreadSafeDict = make_thread_safe_class(dict, method_names=(
    '__getitem__', '__setitem__', '__contains__',
    'get', 'set', 'pop', 'popitem', 'setdefault', 'update'
))


SQL_PATTERNS = {
    'select': re.compile(r'select\s.*?\sfrom\s+[`"]?(?P<table>\w+)[`"]?',
                         re.I | re.S),
    'insert': re.compile(r'insert\s+(ignore\s+)?(into\s+)?[`"]?(?P<table>\w+)[`"]?',
                         re.I),
    'update': re.compile(r'update\s+(ignore\s+)?[`"]?(?P<table>\w+)[`"]?\s+set',
                         re.I),
    'replace': re.compile(r'replace\s+(into\s+)?[`"]?(?P<table>\w+)[`"]?', re.I),
    'delete': re.compile(r'delete\s+from\s+[`"]?(?P<table>\w+)[`"]?', re.I),
}


def parse_execute_sql(sql):
    sql = sql.lstrip()
    cmd = sql.split(' ', 1)[0].lower()

    if cmd not in SQL_PATTERNS:
        raise Exception('SQL command %s is not yet supported' % cmd)  # noqa pragma: no cover

    match = SQL_PATTERNS[cmd].match(sql)
    if not match:
        raise Exception(sql)  # pragma: no cover

    table = match.group('table')

    return cmd, table


def get_thread_ident():
    return threading.currentThread().ident


def car(lst: 'AST') -> str:
    return lst[0]


def cdr(lst: 'AST') -> 'AST':
    return lst[1:]


def optimize_sexp(sexp):
    if not is_sql_ast(sexp):
        return sexp
    head = car(sexp)
    tail = cdr(sexp)
    if head == 'VALUE':
        return sexp
    if head in ('AND', 'OR'):
        if len(tail) == 1:
            return optimize_sexp(tail[0])
        return [head] + [optimize_sexp(x) for x in tail]
    return [optimize_sexp(x) for x in sexp]


def optimize_sql_ast(sql_ast):
    return [optimize_sexp(sexp) for sexp in sql_ast]


def is_sql_ast(lst: 'AST') -> bool:
    if not isinstance(lst, list) or not lst:
        return False
    if not isinstance(lst[0], str_types):
        return False  # pragma: no cover
    return lst[0].upper() == lst[0]


def to_str(cls):
    if '__str__' in cls.__dict__:
        return cls  # pragma: no cover

    def __str__(self):
        return '{}({})'.format(  # pragma: no cover
            self.__class__.__name__,
            ', '.join(
                '{}={}'.format(k, friendly_repr(v))
                for k, v in iteritems(self.__dict__)
            )
        )
    cls.__str__ = __str__
    if '__repr__' not in cls.__dict__:
        cls.__repr__ = cls.__str__
    return cls


@to_str
class ArgSpec(object):
    def __init__(self, argspec):
        self.varargs = argspec.varargs
        if hasattr(argspec, 'varkw'):
            self.varkw = argspec.varkw  # pragma: no cover
            self.kwonlyargs = OrderedDict(  # pragma: no cover
                (k, argspec.kwonlydefaults.get(k))
                for k in argspec.kwonlyargs
            )
        else:
            self.varkw = argspec.keywords  # pragma: no cover
            self.kwonlyargs = OrderedDict()  # pragma: no cover
        args = argspec.args
        defaults = argspec.defaults or []
        dl = len(defaults)
        if dl != 0:
            args = args[: -dl]  # pragma: no cover
            defaults = zip(argspec.args[-dl:], defaults)  # pragma: no cover
        self.args = args
        self.defaults = OrderedDict(defaults)


def getargspec(func):
    if hasattr(inspect, 'getfullargspec'):
        argspec = inspect.getfullargspec(func)  # pragma: no cover
    else:
        argspec = inspect.getargspec(func)  # pragma: no cover
    return ArgSpec(argspec)


def log_call(fmt, logger, level=logging.INFO,
             toggle=lambda *args, **kwargs: True):

    keys = {x[1] for x in Formatter().parse(fmt)}

    def _(func):
        argspec = getargspec(func)

        @wraps(func)
        def __(*args, **kwargs):
            ret = func(*args, **kwargs)

            if not toggle(*args, **kwargs):
                return ret  # pragma: no cover

            vals = dict(argspec.defaults)
            vals.update(dict(izip(argspec.args, args)))
            vals.update(kwargs)
            vals['%ret'] = ret
            vals = {k: v for k, v in iteritems(vals) if k in keys}
            str_ = fmt.format(**vals)
            logger.log(level, str_)
            return ret

        return __

    return _
