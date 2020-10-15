import inspect
import operator
import re
import threading
import weakref
from copy import copy
from datetime import date, datetime
from functools import wraps
from itertools import chain, product
from typing import Tuple, List, Dict, Set, Union, Any, Callable

from six import with_metaclass

from olo._speedups import decrypt_attrs, parse_attrs
from olo.cache import CacheWrapper, delete_cache
from olo.cached_query import CachedQuery
from olo.compat import (str_types, iteritems, iterkeys, itervalues, izip,
                        long, reduce, get_values, xrange)
from olo.context import Context, context, model_instantiate_context
from olo.errors import DeparseError, ExpressionError, InvalidFieldError, ORMError
from olo.events import after_delete, after_insert, after_update, before_update
from olo.expression import Expression
from olo.ext.exported import IS_EXPORTED_PROPERTY
from olo.ext.n import N
from olo.field import BaseField, DbField, Field, UnionField, BatchField
from olo.funcs import RAND
from olo.key import StrKey
from olo.query import Query
from olo.statement import Assignment
from olo.utils import (camel2underscore, deprecation,
                       friendly_repr, missing, override,
                       readonly_cached_property, type_checker)

VALID_TYPES = str_types + (bytes, int, float, long, date, datetime, bool)
PATTERN_N_NAME = re.compile('s$')


def _default_parser(v):
    return v


def _datetime_parser(v):
    return datetime.strftime(v, '%Y-%m-%d %H:%M:%S')


def _date_parser(v):
    return datetime.strftime(v, '%Y-%m-%d')


def _product_order_by_tuples(key):
    strs = key
    l = len(strs)
    if not l:
        return []
    x = list(product(strs, ('-', '')))
    d = len(x) / l
    pieces = [x[int(i * d): int((i + 1) * d)] for i in xrange(l)]
    pieces = list(product(*pieces))
    return [tuple('%s%s' % (t[1], t[0]) for t in p)
            for p in pieces]


def _product_index_keys(key):
    if len(key) == 0:
        yield StrKey()
    for i in xrange(1, len(key) + 1):
        yield StrKey(key[0: i])


class ModelOptions(object):
    def __init__(self, db=None, cache_client=None,
                 cache_key_prefix='olo', cache_expire=60 * 60 * 24,
                 enable_log=False, db_field_version=1,
                 cache_key_version='v0.1.1',
                 query_class=Query,
                 cached_query_class=CachedQuery,
                 cache_class=CacheWrapper,
                 auto_use_cache=False,
                 report=None,
                 table_engine=None,
                 table_charset=None,
                 **kwargs):
        assert db_field_version in (0, 1)
        if db:
            db.enable_log = enable_log
        self.db = db
        self.cache_client = cache_client
        self.cache_key_prefix = cache_key_prefix
        self.cache_expire = cache_expire
        self.cache_key_version = cache_key_version
        self.enable_log = enable_log
        self.db_field_version = db_field_version
        self.query_class = query_class
        self.cached_query_class = cached_query_class
        self.cache_class = cache_class
        self.auto_use_cache = auto_use_cache
        self.table_engine = table_engine
        self.table_charset = table_charset
        self._report = report
        self.update(**kwargs)

    def report(self, *args, **kwargs):
        if self._report:
            return self._report(*args, **kwargs)
        if self.db:
            return self.db.report(*args, **kwargs)

    def update(self, **kwargs):
        options = {k: v for k, v in iteritems(kwargs)
                   if not k.startswith('_')}
        self.__dict__.update(options)


def _process_key(cls, key):
    if isinstance(key, StrKey):
        return key
    if not isinstance(key, (list, tuple, set)):
        key = (key,)
    _key = []
    for item in key:
        if isinstance(item, Field):
            _key.append(item.attr_name)
        elif isinstance(item, str_types):
            attr_name = item
            attr_name = cls.__field_name_map__.get(attr_name, attr_name)
            if not hasattr(cls, attr_name):
                cls._options.report(  # pragma: no cover
                    'The key of {} is error: {}. '
                    'Cannot find the attribute: `{}`'.format(
                        cls.__name__, key, attr_name
                    )
                )
                return  # pragma: no cover
            _key.append(attr_name)
        else:
            raise RuntimeError(  # pragma: no cover
                '{}\'s key is not support this item: `{}`'.format(
                    cls.__name__, repr(item)
                )
            )
    return StrKey(_key)


def _get_keys_from_db(cls):
    primary_keys = []
    unique_keys = []
    index_keys = []

    default = (primary_keys, unique_keys, index_keys)

    db = cls._options.db
    if not db:
        return default

    index_rows = db.get_index_rows(cls._get_table_name())

    pre_is_unique = False
    pre_key_name = None
    pieces = []

    def collect():
        if not pre_key_name:
            return  # pragma: no cover

        if not pieces:
            return  # pragma: no cover

        key = _process_key(
            cls,
            list(map(
                lambda t: t[1],  # noqa
                sorted(pieces, key=lambda t: t[0])  # noqa
            ))
        )

        if key is None:
            return  # pragma: no cover

        if not pre_is_unique:
            index_keys.append(key)
            return

        if pre_key_name == 'PRIMARY':
            primary_keys.append(key)
        else:
            unique_keys.append(key)

    for row in index_rows:
        key_name = row[2]
        seq = row[3]
        column_name = row[4]
        if pre_key_name is not None:
            if pre_key_name != key_name:
                collect()
                pieces = []
        pieces.append((seq, column_name))
        pre_key_name = key_name
        pre_is_unique = row[1] == 0

    collect()

    return primary_keys, unique_keys, index_keys


def init_field(field, cls, attr_name):
    if not field.name:
        field.name = attr_name
    field.attr_name = attr_name
    field._model_ref = weakref.ref(cls)
    field.AES_KEY = getattr(cls, 'AES_KEY', '')


def _collect_fields(cls, attrs) -> Tuple[
    Set[str], Set[str], Set[Union[BaseField, Any]], Dict[str, Any], Set[str], Dict[str, Callable], Set[str], list, Dict[
        Any, str], Dict[str, BaseField], Dict[str, BaseField]]:
    # pylint: disable=too-many-statements
    fields = set()
    db_fields = set()
    primary_key = set()
    on_updates = {}
    choices_field_sets = set()
    validates = {}
    exported_property_names = set()
    field_objs = []
    field_name_map = {}
    encrypted_fields = {}
    setter_fields = {}

    for k in dir(cls) if not cls.__abstract__ else []:
        v = getattr(cls, k)
        if isinstance(v, BaseField):
            if k not in attrs:
                v = v.clone()
                setattr(cls, k, v)
            init_field(v, cls, k)
            if k[:2] != '__' and k[-2:] != '__':
                if isinstance(v, Field):
                    fields.add(k)
                    field_objs.append(v)
                elif isinstance(v, DbField):
                    db_fields.add(k)
                if v.encrypt:
                    encrypted_fields[k] = v
                if v.choices is not None:
                    choices_field_sets.add(k)
                if v._setter is not None:
                    setter_fields[k] = v
            if v.is_primary_key():
                primary_key.add(v)
            if v.on_update is not None:
                on_updates[k] = v.on_update
            field_name_map[v.name] = k
        elif callable(v) and k.startswith('validate_'):
            pieces = k.partition('_')
            field_name = pieces[2]
            if not field_name:
                continue  # pragma: no cover
            validates[field_name] = v
        elif getattr(v, IS_EXPORTED_PROPERTY, False):
            exported_property_names.add(k)
        elif isinstance(v, N):
            name = k
            _name = PATTERN_N_NAME.sub('', name)
            if name == _name or _name in cls.__dict__:
                continue  # pragma: no cover
            f = BatchField(object, default=v.default, name=_name)
            f.getter(v.func)
            setattr(cls, _name, f)

    ordered_field_attr_names = [
        f.attr_name for f in sorted(field_objs, key=lambda f: f.id)
    ]

    return (
        fields, db_fields, primary_key,
        on_updates, choices_field_sets, validates,
        exported_property_names, ordered_field_attr_names,
        field_name_map, encrypted_fields, setter_fields,
    )


class ModelIter(object):
    def __init__(self, model):
        self.model = model

    def next(self):
        raise TypeError(  # pragma: no cover
            'Use select(...) function or %s.select(...) method for iteration'
            % self.model.__name__
        )

    __next__ = next


class ModelMeta(type):
    _finals = set()

    def __new__(mcs, class_name, bases, attrs):
        finals = []
        for k, v in iteritems(attrs):
            if (
                k in mcs._finals and
                not getattr(v, '_override', False)
            ):
                finals.append(k)
        if finals:
            raise RuntimeError(
                'Class `{}` override some final attrs: {}. '
                'Please use the `@override` decorator to decorate them'
                ' if you understand what are you doing'.format(
                    class_name,
                    ', '.join(map('`{}`'.format, finals))
                )
            )

        options = {}

        for base in bases:
            meta = getattr(base, 'Options', None)
            if meta is not None:
                options.update(meta.__dict__)

        meta = attrs.get('Options', None)
        if meta:
            options.update(meta.__dict__)

        options = {
            k: v for k, v in iteritems(options)
            if not k.startswith('_')
        }

        attrs['Options'] = type('Options', (), options)
        attrs['_options'] = ModelOptions(**options)
        if '__abstract__' not in attrs:
            attrs['__abstract__'] = False

        return super(ModelMeta, mcs).__new__(mcs, class_name, bases, attrs)

    def __init__(cls, class_name, bases, attrs):
        # pylint: disable=too-many-statements
        super(ModelMeta, cls).__init__(class_name, bases, attrs)

        if cls.__abstract__:
            return

        (
            fields,
            db_fields,
            primary_key,
            on_updates,
            choices_field_sets,
            validates,
            exported_property_names,
            ordered_field_attr_names,
            field_name_map,
            encrypted_fields,
            setter_fields,
        ) = _collect_fields(cls, attrs)

        cls.__fields__ = fields
        cls.__db_fields__ = db_fields
        cls.__all_fields__ = fields | db_fields
        cls.__choices_field_sets__ = choices_field_sets
        cls.__validates__ = validates
        cls.__exported_property_names__ = exported_property_names
        cls.__ordered_field_attr_names__ = ordered_field_attr_names
        cls.__field_name_map__ = field_name_map
        cls.__encrypted_fields__ = encrypted_fields
        cls.__setter_fields__ = setter_fields

        if '__table_name__' not in attrs:
            cls.__table_name__ = camel2underscore(class_name)

        if hasattr(cls, '__on_updates__'):
            _on_updates = cls.__on_updates__.copy()
            _on_updates.update(on_updates)
            cls.__on_updates__ = _on_updates
        else:
            cls.__on_updates__ = on_updates

        if not hasattr(cls, '__primary_key__') or not cls.__primary_key__:
            cls.__primary_key__ = primary_key

        uk = getattr(cls, '__unique_key__', None)
        uks = getattr(cls, '__unique_keys__', None) or set()

        uks = set(uks)

        # compat __unique_key__
        if uk:
            uks.add(uk)  # pragma: no cover
            del cls.__unique_key__  # pragma: no cover

        cls.__unique_keys__ = set(filter(lambda x: x is not None, (
            _process_key(cls, uk)
            for uk in uks
        )))
        cls.__primary_key__ = _process_key(cls, cls.__primary_key__)

        index_keys = set()
        order_bys = set()

        for base in bases:
            index_keys |= getattr(base, '__index_keys__', set())
            order_bys |= getattr(base, '__order_bys__', set())

        _, uks, iks = _get_keys_from_db(cls)

        cls.__unique_keys__ |= set(uks)

        # index keys
        _index_keys = set(filter(lambda x: x is not None, (
            _process_key(cls, k)
            for k in attrs.get('__index_keys__', [])
        ))) | set(iks)

        cls.__index_keys__ = index_keys

        for key in _index_keys:
            cls.__index_keys__.update(
                set(_product_index_keys(key))
            )

        # order bys
        _order_bys = {
            (k,) if not isinstance(k, tuple) else k
            for k in attrs.get('__order_bys__', [])
        }

        cls.__order_bys__ = order_bys | _order_bys | (
            set(_product_order_by_tuples(cls.__primary_key__))
        )

        cls._lock = threading.RLock()

        old_init = attrs.get('__init__')

        if old_init and getattr(old_init, '_override', False):
            @wraps(old_init)
            def wrapped_init(self, *args, **kwargs):
                with model_instantiate_context(self.__ctx__):
                    return old_init(self, *args, **kwargs)

            cls.__init__ = wrapped_init
            cls._olo_is_breaked = True

        cls.__ctx__ = Context()

        if cls._options.db is not None:
            cls._options.db.register_model(cls)

    @readonly_cached_property
    def cq(cls):
        return cls._options.cached_query_class(cls)

    @readonly_cached_property
    def query(cls):
        return cls._options.query_class(cls)

    @readonly_cached_property
    def cache(cls) -> CacheWrapper:
        return cls._options.cache_class(cls)

    @readonly_cached_property
    def __sorted_fields__(cls):
        return sorted(cls.__fields__, key=lambda x: getattr(cls, x).id)

    @classmethod
    def final(mcs, method):
        name = getattr(method, '__name__', None)
        if name and not getattr(method, '_override', False):
            mcs._finals.add(name)
        return method

    def __iter__(cls):
        return ModelIter(cls)


def final_methods(cls):
    for v in itervalues(cls.__dict__):
        if inspect.isfunction(v):
            ModelMeta.final(v)
    return cls


@final_methods
class Model(with_metaclass(ModelMeta)):

    AES_KEY = '*' * 32

    __abstract__ = True

    _olo_is_new = True
    _olo_qs = None
    _olo_qs_idx = 0

    def __init__(self, _olo_is_new=None, _olo_decrypt=True, **attrs):
        depth = 0
        if getattr(self.__class__, '_olo_is_breaked', False):
            depth = self.__ctx__.instantiate_depth or 0
        if not isinstance(_olo_is_new, bool):
            _olo_is_new = depth <= 1
        self._olo_is_new = _olo_is_new
        self._olo_decrypt = _olo_decrypt and not self._olo_is_new

        if self._olo_is_new:
            self._check_attrs(attrs)
            attrs = self._wash_attrs(attrs)
            attrs = self._olo_append_default_attrs(attrs)

        if self.__encrypted_fields__ and self._olo_decrypt:
            attrs = decrypt_attrs(self.__class__, attrs)

        self._init()
        self._data = attrs

        if self._olo_is_new:
            for k in self.__setter_fields__:
                v = attrs.get(k, missing)
                if v is missing:
                    continue
                setattr(self, k, v)

    def _init(self):
        self._parsed_data = {}
        self._dirty_fields = set()
        self._orig = None

    def _clone(self):
        r = copy(self)
        for k, v in iteritems(self._data):
            if k in self.__db_fields__:
                r._data[k] = copy(v)
        return r

    def _set_orig(self):
        if self._olo_is_new:
            return
        self._orig = self._clone()

    @override
    def get_uuid(self):
        raise NotImplementedError

    def get_finally_uuid(self):
        uuid = self.get_uuid()
        return '{}/props'.format(uuid)

    def __getstate__(self):
        dct = dict(self.__dict__)
        dct.pop('_dirty_fields', None)
        dct.pop('_orig', None)
        dct.pop('_parsed_data', None)
        dct = dict(dct)
        _data = dct.get('_data', {})
        if _data:
            dct['_data'] = {
                k: v
                for k, v in iteritems(_data)
                if k not in self.__db_fields__
            }
        # Return tuple to distinguish the old version
        return (dct,)

    def __setstate__(self, state):
        if isinstance(state, tuple):
            self.__dict__.update(state[0])
        else:
            self._data = state  # pragma: no cover
        self._init()

    @classmethod
    def _olo_instantiate(cls, **attrs):
        _olo_is_new = attrs.pop('_olo_is_new', False)
        return cls._instantiate(_olo_is_new=_olo_is_new, **attrs)

    @classmethod
    def _instantiate(cls, **attrs):
        return cls(**attrs)

    @classmethod
    def _check_choices(cls, attrs):
        for field_name in cls.__choices_field_sets__:
            v = attrs.get(field_name, missing)
            if v is not missing:
                getattr(cls, field_name).validate(v)

    def _check_validates(self, attrs):
        for field_name, validate in iteritems(self.__validates__):
            v = attrs.get(field_name, missing)
            if v is not missing:
                validate(v)
        self.olo_validate()

    def _validate_attrs(self, attrs, parse=True, decrypt=True, output=True):
        if parse:
            parsed_attrs = self._parse_attrs(
                attrs, decrypt=decrypt, output=output
            )
        else:
            parsed_attrs = attrs  # pragma: no cover
        self._check_choices(parsed_attrs)
        self._check_validates(parsed_attrs)
        return parsed_attrs

    def _clear_cache(self):
        delete_cache(self)

    def _rollback(self):
        if self._orig:
            self._data.update(self._orig._data)
            self._init()

    def is_dirty(self):
        return bool(self._dirty_fields)

    def save(self):
        if self._olo_is_new:
            return self._olo_insert()
        if not self._dirty_fields:
            return False
        attrs = {key: getattr(self, key) for key in self._dirty_fields}
        is_success = self.update(**attrs)
        self._dirty_fields.clear()
        return is_success

    def update(self, **attrs):
        # pylint: disable=too-many-statements
        self._check_attrs(attrs)

        attrs = self._wash_attrs(attrs)

        if not attrs:
            return False

        if self._orig is None:
            self._set_orig()

        if self.before_update(**attrs) is False:
            self._rollback()
            return False

        for k in self.__setter_fields__:
            v = attrs.get(k, missing)
            if v is missing:
                continue
            f = getattr(self.__class__, k)
            v = f._setter(self, v)
            attrs[k] = v

        db = self._get_db()

        need_updates = {}
        for k, v in iteritems(self.__on_updates__):
            if k in attrs:
                continue

            try:
                res = v()
            except TypeError:
                res = v(self)

            need_updates[k] = res

        attrs = dict(need_updates, **attrs)
        assignments, sql_attrs, db_attrs = self._split_attrs(attrs)

        sql_attrs = self._validate_attrs(sql_attrs, decrypt=False)
        db_attrs = self._validate_attrs(db_attrs, decrypt=False)
        clean_attrs = dict(sql_attrs, **db_attrs)

        for k in db_attrs:
            # cache old db values
            getattr(self._orig, k, None)

        next_inst = self._clone()
        next_inst.__setstate__(dict(self._data, **clean_attrs))
        can_update = self._orig._will_update(
            next_inst,
            fields=clean_attrs.keys(),
        )
        if can_update is False:
            self._rollback()
            return False

        if assignments:
            expression = self.unique_expression
            if expression is None:
                raise ExpressionError('Cannot update this instance because of '  # noqa pragma: no cover
                                      'the model has no primary_key '
                                      'and unique_key')

            sql_ast = [
                'UPDATE',
                ['TABLE', self._get_table_name()],
                ['SET',
                 ['SERIES'] + [asg.get_sql_ast() for asg in assignments]],
                ['WHERE'] + [expression.get_sql_ast()]
            ]

            with db.transaction():
                db.ast_execute(sql_ast)

            dynamic_exps = [
                asg for asg in assignments if isinstance(asg.right, Expression)
            ]
            if dynamic_exps:
                keys = list(map(lambda x: x.left.attr_name, dynamic_exps))
                q = self.__class__.query(*keys).filter(**{
                    attr_name: getattr(self, attr_name)
                    for attr_name in self.__primary_key__
                })
                values = q.first()
                if not isinstance(values, tuple):
                    values = [values]
                _attrs = dict(izip(keys, values))
                sql_attrs.update(self._parse_attrs(_attrs))

        before_update.send(self)

        clean_attrs = dict(sql_attrs, **db_attrs)
        self._data.update(clean_attrs)
        for k in clean_attrs:
            self._parsed_data.pop(k, None)

        for k, v in iteritems(db_attrs):
            field = getattr(self.__class__, k)
            field.db_set(self, v)

        _orig = self._orig

        def func():
            db.commit_beansdb()
            after_update.send(self)
            self.after_update()
            if _orig is not None:
                self._orig = None
                self._did_update(
                    _orig,
                    fields=chain.from_iterable([
                        iterkeys(sql_attrs),
                        iterkeys(db_attrs),
                    ])
                )

        def rollback_handler():
            self._rollback()

        if db.autocommit:
            func()
        else:
            db.add_lazy_func(func)
            db.add_rollback_handler(rollback_handler)

        return True

    def delete(self, **kwargs):
        # before_delete will return None, so explicit compare with False
        if self.before_delete(**kwargs) is False:
            return False

        expression = self.unique_expression
        if expression is None:
            raise ExpressionError('Cannot delete this instance because of '  # noqa pragma: no cover
                                  'the model has no primary_key '
                                  'and unique_key')

        sql_ast = [
            'DELETE',
            ['TABLE', self._get_table_name()],
            ['WHERE'] + [expression.get_sql_ast()]
        ]

        db = self._get_db()

        def func():
            after_delete.send(self)
            self.after_delete(**kwargs)

        with db.transaction():
            db.ast_execute(sql_ast)
            db.add_lazy_func(func)

        return True

    @override
    def to_json(self):
        return self.to_dict(jsonize=True)

    @override
    def to_dict(self, excludes=None, parsers=None,
                type_parsers=None, jsonize=False):
        excludes = excludes or []
        parsers = parsers or {}
        if type_parsers is None:
            type_parsers = {}
        if jsonize:
            type_parsers.update({
                datetime: _datetime_parser,
                date: _date_parser,
            })

        res = {}

        for k in chain(self.__all_fields__, self.__exported_property_names__):
            if k in excludes:
                continue
            v = getattr(self, k)
            if isinstance(v, Field):
                continue  # pragma: no cover
            parser = parsers.get(k, _default_parser)
            parser = type_parsers.get(type(v), parser)
            res[k] = parser(v)

        return res

    @classmethod
    def _olo_get_field(cls, attr_name):
        if attr_name not in cls.__fields__:
            return
        return getattr(cls, attr_name)

    @classmethod
    def _olo_get_db_field(cls, attr_name):
        if attr_name not in cls.__db_fields__:
            return  # pragma: no cover
        return getattr(cls, attr_name)

    @classmethod
    def _split_attrs(cls, attrs, collect_assignment=True) -> Tuple[List[Assignment], Dict, Dict]:
        assignments = []
        sql_attrs = {}
        db_attrs = {}
        for k, v in iteritems(attrs):
            if k in cls.__db_fields__:
                db_attrs[k] = v
            elif k in cls.__fields__:
                if not isinstance(v, Expression):
                    sql_attrs[k] = v
                if collect_assignment:
                    f: Field = getattr(cls, k)
                    v = cls._deparse_attrs({k: v})[k]
                    assignments.append(Assignment(f, v))
        return assignments, sql_attrs, db_attrs

    @classmethod
    def _check_attrs(cls, attrs):
        key = '_olo_dir_cache'

        cls_attrs = cls.__dict__.get(key)
        if cls_attrs is None:
            cls_attrs = set(dir(cls))
            setattr(cls, key, cls_attrs)

        invalid_attrs = set(attrs) - cls_attrs

        if invalid_attrs:
            raise InvalidFieldError(
                'Cannot found the attributes from {}: {}'.format(
                    cls.__name__,
                    ', '.join(invalid_attrs)
                )
            )

    @classmethod
    def _wash_attrs(cls, attrs):
        return {
            k: v
            for k, v in iteritems(attrs)
            if v is not missing
        }

    @classmethod
    def _map_attrs(cls, attrs):
        return {  # pragma: no cover
            getattr(cls, k).name: v
            for k, v in iteritems(attrs)
        }

    @classmethod
    def create(cls, **attrs):
        inst = cls._olo_instantiate(
            _olo_is_new=True, **attrs
        )
        if inst._olo_insert():
            return inst

    def _olo_insert(self):
        if not self._olo_is_new:
            return False  # pragma: no cover

        before_create_is_instance_method = getattr(self.before_create, '__self__', None) is self  # noqa pylint: disable=C

        bcr = True
        if before_create_is_instance_method:
            bcr = self.before_create()

        attrs = dict(self._data)
        _, sql_attrs, db_attrs = self._split_attrs(attrs)

        if not before_create_is_instance_method:
            bcr = self.before_create(**attrs)  # pragma: no cover

        # bcr will be none so must compare with False!!!
        if bcr is False:  # noqa
            return False

        self._validate_attrs(attrs, parse=True,
                             decrypt=self._olo_decrypt)

        db = self._get_db()

        assignments, _, _ = self._split_attrs(sql_attrs)

        if assignments:
            fields_ast = ['BRACKET']
            values_ast = ['VALUES']

            for asg in assignments:
                fields_ast.append(['QUOTE', asg.left.name])
                values_ast.append(['VALUE', asg.right])

            pk_name = self.get_singleness_pk_name()

            sql_ast = [
                'INSERT',
                ['TABLE', self._get_table_name()],
                fields_ast,
                values_ast,
                ['RETURNING', pk_name],
            ]

            with db.transaction():
                id_ = db.ast_execute(sql_ast)

            if (
                    hasattr(self.__class__, pk_name) and
                    pk_name in self.__class__.__fields__ and
                    pk_name not in self._data
            ):
                self._data[pk_name] = id_

            # need thinking
            self._extend_missing_data()

        for k, v in iteritems(db_attrs):
            field = getattr(self.__class__, k)
            field.db_set(self, v)

        self._olo_is_new = False

        def rollback_handler():
            self._olo_is_new = True

        def func():
            db.commit_beansdb()
            after_insert.send(self)
            if getattr(self.after_create, '__self__', None) is self:
                self.after_create()
            else:
                self.after_create(self)  # pragma: no cover pylint: disable=E

        if db.autocommit:
            func()
        else:
            db.add_lazy_func(func)
            db.add_rollback_handler(rollback_handler)

        return True

    @classmethod
    def get_singleness_pk_attr_name(cls):
        pk = cls.__primary_key__
        pkl = len(pk)
        if pkl != 1:
            raise ExpressionError(
                'This method only support singleness primary key now. '
                'But your primary key has {} keys'.format(pkl)
            )
        return list(pk)[0]

    @classmethod
    def get_singleness_pk_field(cls):
        attr_name = cls.get_singleness_pk_attr_name()
        return getattr(cls, attr_name)

    @classmethod
    def get_singleness_pk_name(cls, default='id'):
        try:
            field = cls.get_singleness_pk_field()
        except ExpressionError:
            return default
        return field.name

    def _get_singleness_pk_value(self):
        field = self.get_singleness_pk_field()
        return getattr(self, field.attr_name)

    def _olo_get_pk_value(self):
        pk = self.__primary_key__
        return tuple(getattr(self, attr_name) for attr_name in pk)

    def _olo_get_signature(self):
        pk_value = self._olo_get_pk_value()
        return (self.__table_name__,) + pk_value

    def _extend_missing_data(self):
        missing_fields = [
            getattr(self.__class__, k) for k in self.__fields__
            if k not in self._data
        ]
        if not missing_fields:
            return  # pragma: no cover
        pk_dict = self._get_pk_dict()
        if not pk_dict:
            raise ORMError('No pk dict!!!')  # pragma: no cover
        values = self.__class__.query(*missing_fields).filter(
            **pk_dict
        ).first()
        if len(missing_fields) == 1 and values is not None and not isinstance(values, list):  # noqa
            values = [values]  # pragma: no cover
        if values:
            self._data.update(
                dict(izip(map(lambda f: f.attr_name, missing_fields), values))
            )

    def _get_pk_dict(self):
        dct = {}
        for attr_name in self.__primary_key__:
            v = getattr(self, attr_name, missing)
            if v is not missing:
                dct[attr_name] = v
        return dct

    @classmethod
    def _get(cls, id=None, **kwargs):
        if not kwargs:
            pk_name = cls.get_singleness_pk_name()
            return cls._get_by(**{pk_name: id})
        return cls._get_by(**kwargs)

    @classmethod
    def get(cls, id=None, **kwargs):
        opt = cls._options
        if opt.cache_client and opt.auto_use_cache:
            return cls.cache.get(id=id, **kwargs)
        return cls._get(id=id, **kwargs)

    @classmethod
    def _get_multi(cls, idents, filter_none=True):
        if not idents:
            return []
        if not type_checker([dict], idents):
            pk_name = cls.get_singleness_pk_name()
            pk_field = getattr(cls, pk_name)
            items = cls._get_multi_by(pk_field.in_(idents))
            mapping = {str(getattr(item, pk_name)): item for item in items}
        else:
            ident = idents[0]
            items = cls._get_multi_by(
                UnionField(*[getattr(cls, k) for k in ident])
                .in_([get_values(_ident) for _ident in idents])
            )
            mapping = {
                tuple(
                    str(getattr(item, k))
                    for k in ident
                ): item
                for item in items
            }
        res = []
        for ident in idents:
            if not isinstance(ident, dict):
                item = mapping.get(str(ident))
            else:
                item = mapping.get(tuple(map(str, ident.values())))
            if item is None and filter_none:
                continue
            res.append(item)
        return res

    @classmethod
    def get_multi(cls, idents, filter_none=True):
        opt = cls._options
        if opt.cache_client and opt.auto_use_cache:
            return cls.cache.get_multi(idents, filter_none=filter_none)
        return cls._get_multi(idents, filter_none=filter_none)

    @classmethod
    def gets(cls, idents, filter_none=True):
        deprecation(
            'The class method Model.gets is deprecated, '
            'please use Model.get_multi!'
        )
        return cls.get_multi(idents, filter_none=filter_none)

    @classmethod
    def _get_by(cls, *expressions, **expression_dict):
        return cls.query.filter(*expressions, **expression_dict).one()

    @classmethod
    def get_by(cls, *expressions, **expression_dict):
        opt = cls._options
        if opt.cache_client and opt.auto_use_cache:
            return cls.cache.get_by(*expressions, **expression_dict)
        return cls._get_by(*expressions, **expression_dict)

    @classmethod
    def get_multi_by_random(cls, *expressions, **expression_dict):
        expression_dict.update({'order_by': RAND()})  # pragma: no cover
        return cls.gets_by(*expressions, **expression_dict)  # pragma: no cover

    @classmethod
    def gets_by_random(cls, *expressions, **expression_dict):
        deprecation(  # pragma: no cover
            'The class method Model.gets_by_random is deprecated, '
            'please use Model.get_multi_by_random!'
        )
        return cls.get_multi_by_random(*expressions, **expression_dict)  # noqa pragma: no cover

    @classmethod
    def _get_multi_by(cls, *expressions, **expression_dict):
        return cls._get_multi_by_with_query(cls.query, *expressions,
                                            **expression_dict)

    @classmethod
    def get_multi_by(cls, *expressions, **expression_dict):
        opt = cls._options
        if opt.cache_client and opt.auto_use_cache:
            return cls.cache.get_multi_by(*expressions, **expression_dict)
        return cls._get_multi_by(*expressions, **expression_dict)

    @classmethod
    def gets_by(cls, *expressions, **expression_dict):
        deprecation(
            'The class method Model.gets_by is deprecated, '
            'please use Model.get_multi_by!'
        )
        return cls.get_multi_by(*expressions, **expression_dict)

    @classmethod
    def get_entities_by(cls, entities, *expressions, **expression_dict):
        return cls._get_multi_by_with_query(cls.query(*entities),
                                            *expressions, **expression_dict)

    @classmethod
    def _get_multi_by_with_query(cls, query, *expressions, **expression_dict):
        start = expression_dict.pop('start', None)
        limit = expression_dict.pop('limit', None)
        order_by = expression_dict.pop('order_by', None)
        group_by = expression_dict.pop('group_by', None)

        q = query
        if start is not None:
            q = q.offset(start)
        if limit is not None:
            q = q.limit(limit)
        if order_by is not None:
            if not isinstance(order_by, (list, tuple)):
                order_by = [order_by]
            q = q.order_by(*order_by)
        if group_by is not None:
            if not isinstance(group_by, (list, tuple)):
                group_by = [group_by]
            q = q.group_by(*group_by)

        return q.filter(*expressions, **expression_dict).all()

    @classmethod
    def _count_by(cls, *expressions, **expression_dict):
        return cls.query.filter(*expressions, **expression_dict).count()

    @classmethod
    def count_by(cls, *expressions, **expression_dict):
        opt = cls._options
        if opt.cache_client and opt.auto_use_cache:
            return cls.cache.count_by(*expressions, **expression_dict)
        return cls._count_by(*expressions, **expression_dict)

    @classmethod
    def _get_table_name(cls):
        return cls.__table_name__

    @classmethod
    def _get_db(cls):
        return cls._options.db

    @classmethod
    def get_sql_ast(cls):
        table_name = cls._get_table_name()
        attr_name = '_{}_as_{}_sql_ast'.format(
            table_name, (context.table_alias_mapping or {}).get(table_name)
        )
        if attr_name not in cls.__dict__:
            sql_ast = ['SERIES'] + list(
                map(lambda x: getattr(cls, x).get_sql_ast(),
                    cls.__sorted_fields__)
            )
            setattr(cls, attr_name, sql_ast)
        return getattr(cls, attr_name)

    @classmethod
    def _parse_attrs(cls, attrs, decrypt=True, output=True):
        return parse_attrs(cls, attrs, decrypt=decrypt, output=output)

    @classmethod
    def _olo_append_default_attrs(cls, attrs, fields=None):
        if fields is None:
            fields = cls.__all_fields__

        res = {}
        for k in fields:
            field = getattr(cls, k)
            if k in attrs:
                v = attrs[k]
                if (
                    v is None and
                    not field.noneable
                ):
                    v = field.get_default()
            elif field.default is not None:
                v = attrs[k] = field.get_default()
            else:
                continue  # pragma: no cover
            res[k] = v
        return res

    @classmethod
    def _deparse_attrs(cls, attrs):
        res = {}
        for k, v in iteritems(attrs):
            field = getattr(cls, k)
            is_field = isinstance(field, Field)
            if isinstance(v, Expression):
                res[k] = v
                continue
            if v is None and field.noneable:
                res[k] = v
                continue
            if is_field and not isinstance(v, VALID_TYPES):
                v = field.deparse(v)
            if is_field and not isinstance(v, VALID_TYPES):
                raise DeparseError(  # pragma: no cover
                    'The deparsed type of {}.{} is invalid. '
                    'Type: {}; Value: {}. '
                    'Please check the deparser of this field.'.format(
                        cls.__name__, k, type(v), repr(v)
                    )
                )
            v = field.encrypt_func(v) if field.encrypt else v
            v = field.input(v) if field.input else v
            res[k] = v
        return res

    @classmethod
    def _gen_cache_key(cls, _olo_suffix='_olo_data', **kwargs):
        suffix = _olo_suffix
        old_kwargs = dict(kwargs)
        _kwargs = dict(kwargs)
        _kwargs.pop('order_by', None)
        kwargs = cls._parse_attrs(_kwargs)
        old_kwargs.update(kwargs)
        key = '{}:db:{}:({}):{}'.format(
            cls._options.cache_key_prefix,
            cls._get_table_name(),
            ','.join('{}={}'.format(k, repr(v)) for k, v in sorted(
                iteritems(old_kwargs)
            )),
            cls._options.cache_key_version
        )
        if suffix:
            key += ':suffix:%s' % suffix
        # avoid mc bug
        return key.replace(' ', '&nbsp;')

    @property
    def unique_expression(self):
        keys = []

        if self.__primary_key__:
            keys.extend(self.__primary_key__)
        elif self.__unique_keys__:  # pragma: no cover
            keys = self.__unique_keys__[0]  # pragma: no cover

        if not keys:
            return None  # pragma: no cover

        return reduce(
            operator.and_,
            map(lambda k: getattr(self.__class__, k) == getattr(self, k), keys)
        )

    def __repr__(self):
        class_name = self.__class__.__name__
        return '{class_name}({kwargs})'.format(
            class_name=class_name,
            kwargs=', '.join(
                '{}={}'.format(k, friendly_repr(getattr(self, k)))
                for k in self.__ordered_field_attr_names__
            )
        )

    __str__ = __repr__

    @override
    def will_update(self, next_inst):
        return True

    def _will_update(self, next_inst, fields=None):
        can_update = self.will_update(next_inst)
        if can_update is False:
            return False

        for k in fields or self._dirty_fields:
            method_name = '{}_will_update'.format(k)
            method = getattr(self, method_name, None)
            if not callable(method):
                continue
            v = getattr(self, k, None)
            next_v = getattr(next_inst, k, None)
            if v != next_v:
                # pylint: disable=E1102
                can_update = method(next_v)
                # pylint: enable=E1102
                if can_update is False:
                    return False
        return True

    @override
    def did_update(self, pre_inst):
        pass

    def _did_update(self, pre_inst, fields=None):
        self.did_update(pre_inst)
        for k in fields or self._dirty_fields:
            method_name = '{}_did_update'.format(k)
            method = getattr(self, method_name, None)
            if not callable(method):
                continue
            v = getattr(self, k, None)
            pre_v = getattr(pre_inst, k, None)
            if v != pre_v:
                # pylint: disable=E1102
                method(pre_v)
                # pylint: enable=E1102

    @override
    def after_update(self):
        pass

    @override
    def before_update(self, **attrs):
        pass

    @override
    def before_create(self):
        pass

    @override
    def after_create(self):
        pass

    @override
    def before_delete(self, **kwargs):
        pass

    @override
    def after_delete(self, **kwargs):
        pass

    @override
    def olo_validate(self):
        pass
