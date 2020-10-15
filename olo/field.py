# coding=utf-8
from __future__ import annotations

import json
from collections import defaultdict
from copy import copy
from enum import Enum
from inspect import isfunction
from typing import TYPE_CHECKING, Optional, Callable

from olo.orm_types import TrackedValue
from olo.types.json import JSONLike

if TYPE_CHECKING:
    from olo.model import Model

from olo.compat import int_types, iteritems, izip, str_types
from olo.interfaces import SQLASTInterface
from olo.expression import Expression, UnaryExpression, BinaryExpression
from olo.libs.aes import encrypt, decrypt
from olo.mixins.operations import UnaryOperationMixin, BinaryOperationMixin
from olo.utils import transform_type, missing, type_checker
from olo.funcs import attach_func_method
from olo.errors import ValidationError, DbFieldVersionError
from olo.migration import MigrationVersion

__all__ = ['Field', 'UnionField', 'DbField']


class BaseField(object):

    id = 0

    def __init__(self, type_,
                 default=None, name=None,
                 parser=None, deparser=None,
                 primary_key=False, on_update=None,
                 choices=None, encrypt=False,
                 input=None, output=None,
                 noneable=False, attr_name=None,
                 version=None, length=None,
                 auto_increment=False,
                 charset=None):

        if default is not None and noneable:
            noneable = False

        # auto increment id
        self.id = self.__class__.id + 1
        self.__class__.id = self.id

        self.type = type_
        self.name = name
        self.default = default
        self._parser = (
            parser if parser is not None
            else lambda x: transform_type(x, self.type)
        )
        self._deparser = (
            deparser if deparser is not None
            else lambda x: transform_type(x, str)
        )
        self._primary_key = primary_key
        self.on_update = on_update
        self.choices = choices
        self.encrypt = encrypt
        self.input = input
        self.output = output
        self.noneable = noneable if not primary_key else False
        self.attr_name = attr_name or name
        self.version = version
        self.length = length
        self.auto_increment = auto_increment
        self.charset = charset
        self._alias_name = None
        self._model_ref = lambda: None
        self.AES_KEY = ''
        self._getter = None
        self._setter = None

        if self.choices is None and isinstance(self.type, type) and issubclass(self.type, Enum):
            self.choices = self.type

    def __hash__(self):
        return self.id

    def get_model(self) -> Optional[Model]:
        if not self._model_ref:
            return
        return self._model_ref()

    @property
    def table_name(self):
        model = self.get_model()
        if not model:
            return
        return model._get_table_name()

    def get_default(self):
        return self.default() if callable(self.default) else self.default

    def encrypt_func(self, v):
        return encrypt(v, self.AES_KEY)

    def decrypt_func(self, v):
        return decrypt(v, self.AES_KEY)

    def parse(self, value):
        try:
            return self._parser(value)
        except (TypeError, ValueError):
            return value

    def deparse(self, value):
        try:
            return self._deparser(value)
        except (TypeError, ValueError):
            return value

    def is_primary_key(self) -> bool:
        return self._primary_key

    def validate(self, value):
        if self.choices is None:
            return
        if value not in self.choices:
            raise ValidationError(
                '{} is not a valid choice of `{}`. The choices is: {}'.format(
                    repr(value),
                    self.name,
                    repr(self.choices)
                )
            )

    def __getstate__(self):
        state = self.__dict__.copy()
        for k, v in iteritems(state):
            if isinstance(v, (list, set, dict)):
                v = type(v)(v)
                state[k] = v
        return state

    def __setstate__(self, state):
        self.type = state['type']
        self.__dict__.update(state)

    def clone(self):
        return copy(self)

    @classmethod
    def _get_data(cls, obj):
        return obj._data

    @classmethod
    def _get_parsed_data(cls, obj):
        return obj._parsed_data

    def mark_dirty(self, obj):
        obj._dirty_fields.add(self.attr_name)

    def getter(self, func):
        self._getter = func
        return self

    def setter(self, func):
        self._setter = func
        return self

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        v = self._get(obj, objtype)
        if self._getter is not None:
            return self._getter(obj, v)
        return v

    def _get(self, obj, objtype):
        attr_name = self.attr_name
        data = self._get_data(obj)
        parsed_data = self._get_parsed_data(obj)
        if attr_name in parsed_data:
            return parsed_data[attr_name]
        v = data.get(attr_name, missing)
        if v is not missing:
            if isinstance(v, Expression):
                return v
            if not type_checker(self.type, v) and (v is not None or not self.noneable):
                v = transform_type(v, self.type)
            if self.output:
                v = self.output(v)
            parsed_data[attr_name] = v
            return v
        if self.noneable or obj._olo_is_new:
            return
        raise AttributeError(  # pragma: no cover
            'Cannot found `{}` from `{}` instance'.format(
                self.name, obj.__class__.__name__
            )
        )

    def __set__(self, obj, value):
        if obj is None:
            raise AttributeError(  # pragma: no cover
                'Cannot change the class attr: `{}`'.format(
                    self.name
                )
            )
        if self._setter is not None:
            value = self._setter(obj, value)
        return self._set(obj, value)

    def _set(self, obj, value):
        if obj._orig is None:
            obj._set_orig()
        attr_name = self.attr_name
        data = self._get_data(obj)
        parsed_data = self._get_parsed_data(obj)
        attrs = {attr_name: value}
        parsed_attrs = obj._validate_attrs(attrs, decrypt=False, output=False)
        parsed_value = parsed_attrs[attr_name] if parsed_attrs else value
        old_value = data.get(attr_name, missing)
        if (
            not isinstance(old_value, BaseField) and
            old_value != parsed_value and
            attr_name not in obj._dirty_fields and
            not obj._olo_is_new
        ):
            self.mark_dirty(obj)
        data[attr_name] = parsed_value
        parsed_data.pop(attr_name, None)

    def __repr__(self):
        return '{}(type={}, name={})'.format(
            self.__class__.__name__,
            repr(self.type),
            repr(self.name),
        )


def process_enum_seq(v):
    if isinstance(v, (list, tuple, set)):
        t = type(v)
        return t(i.name if isinstance(i, Enum) else i for i in v)
    return v


@attach_func_method
class Field(BaseField, UnaryOperationMixin, BinaryOperationMixin,
            SQLASTInterface):

    UnaryExpression = UnaryExpression
    BinaryExpression = BinaryExpression

    def __init__(self, *args, **kwargs):
        super(Field, self).__init__(*args, **kwargs)

    def in_(self, other):
        seq = process_enum_seq(other)
        return super().in_(seq)

    def not_in_(self, other):
        seq = process_enum_seq(other)
        return super().not_in_(seq)

    def alias(self, name):
        inst = self.clone()
        inst._alias_name = name
        return inst

    def get_sql_ast(self):
        model = self.get_model()
        table_name = None if not model else model._get_table_name()
        sql_ast = [
            'COLUMN',
            table_name,
            self.name
        ]
        if self._alias_name:
            sql_ast = ['ALIAS', sql_ast, self._alias_name]
        return sql_ast


class ConstField(Field):

    def __init__(self, value, *args, **kwargs):
        self._value = value
        super(ConstField, self).__init__(type(value), *args, **kwargs)

    def get_sql_ast(self):
        return ['VALUE', self._value]


class UnionField(BaseField, UnaryOperationMixin, BinaryOperationMixin, SQLASTInterface):

    UnaryExpression = UnaryExpression
    BinaryExpression = BinaryExpression

    def __init__(self, *fields):
        self.fields = fields
        super(UnionField, self).__init__(Field)
        self.attr_name = '({})'.format(tuple(f.attr_name for f in self.fields))

    def get_sql_ast(self):
        return ['BRACKET'] + [f.get_sql_ast() for f in self.fields]


def _get_json_type(i):
    if isinstance(i, bool):
        return 'boolean'
    if isinstance(i, int):
        return 'int'
    if isinstance(i, float):
        return 'double precision'
    return 'text'


class JSONField(Field):

    def __init__(self, *args, **kwargs):
        super().__init__(JSONLike, *args, **kwargs)
        self._args = args
        self._kwargs = kwargs
        self._path = []
        self._type = None

    def _clone(self):
        inst = self.__class__(*self._args, **self._kwargs)
        inst._path = list(self._path)
        inst._type = self._type
        return inst

    def __getitem__(self, item):
        inst = self.clone()
        inst._path.append(item)
        return inst

    def __eq__(self, other):
        inst = self.clone()
        inst._type = _get_json_type(other)
        return BinaryExpression(inst, other, '=')

    def __ne__(self, other):
        inst = self.clone()
        inst._type = _get_json_type(other)
        operator = '!='
        if other is None:
            operator = 'IS NOT'
        return self.BinaryExpression(inst, other, operator)

    def __gt__(self, other):
        inst = self.clone()
        inst._type = _get_json_type(other)
        return self.BinaryExpression(inst, other, '>')

    def __ge__(self, other):
        inst = self.clone()
        inst._type = _get_json_type(other)
        return self.BinaryExpression(inst, other, '>=')

    def __lt__(self, other):
        inst = self.clone()
        inst._type = _get_json_type(other)
        return self.BinaryExpression(inst, other, '<')

    def __le__(self, other):
        inst = self.clone()
        inst._type = _get_json_type(other)
        return self.BinaryExpression(inst, other, '<=')

    def contains_(self, item):
        return BinaryExpression(self, item, '?')

    def not_contains_(self, item):
        return UnaryExpression(BinaryExpression(self, item, '?'), 'NOT', suffix=False)

    def __and__(self, other):
        return BinaryExpression(self, other, '?&')

    def __or__(self, other):
        return BinaryExpression(self, other, '?|')

    def get_sql_ast(self):
        model = self.get_model()
        table_name = None if not model else model._get_table_name()
        sql_ast = [
            'COLUMN',
            table_name,
            self.name,
            self._path,
            self._type,
        ]
        if self._alias_name:
            sql_ast = ['ALIAS', sql_ast, self._alias_name]
        return sql_ast

    def __get__(self, obj, objtype):
        v = super().__get__(obj, objtype)
        if obj is not None:
            v = TrackedValue.make(obj, self.attr_name, v)
            obj._parsed_data[self.attr_name] = v
        return v


def _make_db_field_key(uuid, attr_name):
    return '{}/{}'.format(uuid, attr_name)


def _get_db_field_version(field, obj):
    if field.version is not None:
        return field.version
    return obj._options.db_field_version


def _prefetch_db_data(obj):
    if not obj:
        return  # pragma: no cover
    session = getattr(obj, '_olo_qs', None)
    if session is None:
        return
    entities = session.entities
    if len(entities) < 2:
        return
    qs_idx = getattr(obj, '_olo_qs_idx', None)
    qs_idx = 0 if not isinstance(qs_idx, int_types) else qs_idx
    if qs_idx < 1:
        return
    pairs = []
    first = entities[0]
    for entity in entities[qs_idx:]:
        need_feed = (
            set(DbField._get_data(first)) -
            set(DbField._get_data(entity))
        )
        for attr_name in need_feed:
            field = entity._olo_get_db_field(attr_name)
            version = _get_db_field_version(field, entity)
            pairs.append((entity, field, version))
    db_values = _get_db_values(pairs)
    if not db_values:
        return
    for (entity, field, version), db_value in izip(pairs, db_values):
        data = DbField._get_data(entity)
        if db_value is missing:
            data[field.attr_name] = None
            continue
        value = _get_value_from_db_value(db_value, version, field)
        data[field.attr_name] = _process_value(value, field)


def _get_db_values(pairs):
    version_groups = defaultdict(list)
    for obj, field, version in pairs:
        assert obj is not None
        assert field is not None
        if isinstance(version, MigrationVersion):
            continue  # pragma: no cover
        version_groups[version].append((obj, field))
    if not version_groups:
        return
    mapping = {}
    for version, _pairs in iteritems(version_groups):
        for obj, field in _pairs:
            if version == 0:
                key = obj.get_finally_uuid()
            elif version == 1:
                key = field._get_db_field_key(obj)
            else:
                continue  # pragma: no cover
            mapping[(obj._olo_get_signature(), field.name, version)] = key
    keys = list(set(mapping.values()))
    if not keys:
        return  # pragma: no cover
    db = obj._get_db()
    values = db.db_get_multi(keys)
    res = []
    for obj, field, version in pairs:
        key = mapping.get((obj._olo_get_signature(), field.name, version))
        if not key:
            value = missing  # pragma: no cover
        else:
            value = values.get(key, missing)
        res.append(value)
    return res


def _get_value_from_db_value(value, version, field):
    if version == 0:
        value = value or {}
        if isinstance(value, str_types):
            value = json.loads(value)
        value = value.get(field.name, missing)
    return value


def _process_value(value, field):
    if value is missing:
        return field.get_default()
    if field is None and not field.noneable:
        return field.get_default()  # pragma: no cover
    return value


class DbField(BaseField):

    def _get_db_field_key(self, obj):
        uuid = obj.get_finally_uuid()
        return _make_db_field_key(uuid, self.name)

    def _get_v0(self, obj, objtype):
        db_values = _get_db_values([(obj, self, 0)])
        db_value = missing if not db_values else db_values[0]
        return _get_value_from_db_value(db_value, 0, self)

    def _get_v1(self, obj, objtype):
        db_values = _get_db_values([(obj, self, 1)])
        db_value = missing if not db_values else db_values[0]
        return _get_value_from_db_value(db_value, 1, self)

    def _set_v0(self, obj, value):
        db = obj._get_db()
        uuid = obj.get_finally_uuid()
        if value is not None or not self.noneable:
            value = transform_type(value, self.type)
        payload = db.db_get(uuid) or {}
        if isinstance(payload, str_types):
            payload = json.loads(payload)
        payload[self.name] = value
        db.db_set(uuid, payload)

    def _set_v1(self, obj, value):
        db = obj._get_db()
        db_key = self._get_db_field_key(obj)
        if value is None and self.noneable:
            # beansdb 不支持 set None
            db.db_delete(db_key)
            return
        value = transform_type(value, self.type)
        db.db_set(db_key, value)

    def _delete_v0(self, obj):
        db = obj._get_db()
        uuid = obj.get_finally_uuid()
        payload = db.db_get(uuid) or {}
        payload.pop(self.name, None)
        db.db_set(uuid, payload)

    def _delete_v1(self, obj):
        db = obj._get_db()
        db_key = self._get_db_field_key(obj)
        db.db_delete(db_key)

    def _get(self, obj, objtype):
        _prefetch_db_data(obj)
        attr_name = self.attr_name
        data = self._get_data(obj)
        if attr_name in data:
            return super(DbField, self)._get(obj, objtype)
        v = self.db_get(obj, objtype)
        data[attr_name] = v
        return v

    def db_get(self, obj, objtype):
        version = _get_db_field_version(self, obj)
        if version == 0:
            v = self._get_v0(obj, objtype)
        elif version == 1:
            v = self._get_v1(obj, objtype)
        elif isinstance(version, MigrationVersion):
            v = getattr(self, '_get_v{}'.format(version.to_version))(obj, objtype)  # noqa
            if v is missing:
                v = getattr(self, '_get_v{}'.format(version.from_version))(obj, objtype)  # noqa
                if v is not missing:
                    self.__set__(obj, v)
        else:
            raise DbFieldVersionError('Invalid version: {}'.format(version))

        return _process_value(v, self)

    def db_set(self, obj, value):
        attr_name = self.attr_name
        attrs = {attr_name: value}
        obj._validate_attrs(
            attrs, decrypt=False, output=False
        )
        version = _get_db_field_version(self, obj)
        if isinstance(version, MigrationVersion):
            version = version.to_version
        if version == 0:
            self._set_v0(obj, value)
        elif version == 1:
            self._set_v1(obj, value)
        else:
            raise DbFieldVersionError('Invalid version: {}'.format(version))

    def __delete__(self, obj):
        if obj is None:
            raise AttributeError(self.attr_name)  # pragma: no cover
        attr_name = self.attr_name
        if obj.before_update(**{
            attr_name: None
        }) is False:
            return
        version = _get_db_field_version(self, obj)
        if version == 0:
            self._delete_v0(obj)
        elif version == 1:
            self._delete_v1(obj)
        elif isinstance(version, MigrationVersion):
            getattr(self, '_delete_v{}'.format(version.to_version))(obj)  # noqa
            getattr(self, '_delete_v{}'.format(version.from_version))(obj)  # noqa
        data = self._get_data(obj)
        parsed_data = self._get_parsed_data(obj)
        data.pop(attr_name, None)
        parsed_data.pop(attr_name, None)
        obj.after_update()


class BatchField(BaseField):
    def __init__(self, type_, default=None, name=None):
        if isfunction(type_):
            type_ = object
        super(BatchField, self).__init__(type_, default=default, name=name, noneable=True)

    def getter(self, func_or_classmethod):
        if isinstance(func_or_classmethod, classmethod):
            func = func_or_classmethod.__func__
        else:
            func = func_or_classmethod
        super(BatchField, self).getter(func)
        return func_or_classmethod

    def __get__(self, instance, owner):
        if instance is None:
            return self

        data = self._get_data(instance)
        v = data.get(self.name, missing)
        if v is not missing:
            return v

        v = self._get(instance, owner)
        data[self.name] = v
        return v

    def _get(self, instance, owner):
        if self._getter is None:
            raise AttributeError('batch field `{}.{}` has no getter!', owner.__name__, self.name)

        session = instance._olo_qs
        if session is None:
            entities = [instance]  # pragma: no cover
        else:
            entities = session.entities

        name = self.name
        default = self.get_default()
        res = self._getter(owner, entities)

        entity_mapping = {
            e._get_singleness_pk_value(): e
            for e in entities
        }

        if isinstance(res, dict):

            for pv, item in iteritems(entity_mapping):
                if hasattr(item, '_olo_qs'):
                    setattr(item, name, res.get(pv, default))

            return res.get(instance._get_singleness_pk_value(), default)

        if isinstance(res, list):

            for idx, item in enumerate(entities):
                if hasattr(item, '_olo_qs'):
                    try:
                        v = res[idx]
                    except IndexError:
                        v = default

                    setattr(item, name, v)

            try:
                return res[instance._olo_qs_idx]
            except IndexError:  # pragma: no cover
                return default  # pragma: no cover

        return default  # pragma: no cover
