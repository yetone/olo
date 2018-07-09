# coding=utf-8

import json
from copy import copy
from collections import defaultdict

from olo.compat import int_types, iteritems, izip, str_types
from olo.interfaces import SQLASTInterface
from olo.expression import Expression, UnaryExpression, BinaryExpression
from olo.libs.aes import encrypt, decrypt
from olo.mixins.operations import UnaryOperationMixin, BinaryOperationMixin
from olo.utils import transform_type, missing, type_checker
from olo.funcs import attach_func_method
from olo.errors import ValidationError, DbFieldVersionError
from olo.migration import MigrationVersion
from olo.context import context


__all__ = ['Field', 'UnionField', 'DbField']


class BaseField(object):

    id = 0

    def __init__(self, type,
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

        self.type = type
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
        self.noneable = noneable
        self.attr_name = attr_name or name
        self.version = version
        self.length = length
        self.auto_increment = auto_increment
        self.charset = charset
        self._alias_name = None
        self._model_ref = lambda: None

    def __hash__(self):
        return self.id

    def get_model(self):
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

    def is_primary_key(self):
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

    def __get__(self, obj, objtype):
        if obj is None:
            return self
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
            obj._dirty_fields.add(attr_name)
        data[attr_name] = parsed_value
        parsed_data.pop(attr_name, None)

    def __repr__(self):
        return '{}(type={}, name={})'.format(
            self.__class__.__name__,
            repr(self.type),
            repr(self.name),
        )


@attach_func_method
class Field(BaseField, UnaryOperationMixin, BinaryOperationMixin,
            SQLASTInterface):

    UnaryExpression = UnaryExpression
    BinaryExpression = BinaryExpression

    def __init__(self, *args, **kwargs):
        super(Field, self).__init__(*args, **kwargs)

    def alias(self, name):
        inst = self.clone()
        inst._alias_name = name
        return inst

    def get_sql_ast(self):
        model = self.get_model()
        table_name = None if not model else model._get_table_name()
        alias_mapping = context.table_alias_mapping or {}
        table_name = alias_mapping.get(table_name, table_name)
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


class UnionField(BaseField, SQLASTInterface):

    def __init__(self, *fields):
        self.fields = fields
        super(UnionField, self).__init__(Field)

    def in_(self, other):
        return BinaryExpression(self, other, 'IN')

    def not_in_(self, other):
        return BinaryExpression(self, other, 'NOT IN')

    def get_sql_ast(self):
        return ['BRACKET'] + [f.get_sql_ast() for f in self.fields]


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
        if db_value is missing:
            continue
        value = _get_value_from_db_value(db_value, version, field)
        data = DbField._get_data(entity)
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
        else:
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

    def __get__(self, obj, objtype):
        if obj is None:
            return self
        _prefetch_db_data(obj)
        attr_name = self.attr_name
        data = self._get_data(obj)
        if attr_name in data:
            return super(DbField, self).__get__(obj, objtype)
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
            raise AttributeError  # pragma: no cover
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
            v = getattr(self, '_delete_v{}'.format(version.to_version))(obj)  # noqa
            v = getattr(self, '_delete_v{}'.format(version.from_version))(obj)  # noqa
        data = self._get_data(obj)
        parsed_data = self._get_parsed_data(obj)
        data.pop(attr_name, None)
        parsed_data.pop(attr_name, None)
        obj.after_update()
