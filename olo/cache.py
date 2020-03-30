import sys
import random
import logging

from functools import wraps

from olo.compat import izip, str_types, iteritems
from olo.events import after_delete, after_insert, after_update, before_update
from olo.expression import UnaryExpression
from olo.field import BaseField
from olo.key import StrKey
from olo.errors import CacheError, ORMError
from olo.logger import logger
from olo.session import QuerySession
from olo.utils import missing, friendly_repr


def wash_kwargs(func):
    @wraps(func)
    def _(self, *args, **kwargs):
        kwargs = self._model_class._wash_attrs(
            kwargs
        )
        return func(self, *args, **kwargs)
    return _


def _order_by_to_str(item):
    if isinstance(item, str_types):
        return item

    if isinstance(item, BaseField):
        return item.attr_name  # pragma: no cover

    if isinstance(item, UnaryExpression):
        _str = (
            item.value.attr_name if hasattr(item.value, 'attr_name')
            else _order_by_to_str(item.value)
        )

        if item.operator == 'DESC':
            return '-{}'.format(_str)

        return _str  # pragma: no cover

    raise ORMError(f'this is not a valid order_by: {item}')


def order_by_to_strs(order_by):
    res = []
    for exp in order_by:
        res.append(_order_by_to_str(exp))
    if not res:
        return None
    return tuple(res)


class CacheWrapper(object):
    MAX_COUNT = 200

    def __init__(self, model_class):
        self._model_class = model_class

    @property
    def _db(self):
        return self._model_class._options.db

    @property
    def _cache_client(self):
        return self._model_class._options.cache_client

    @property
    def _gen_cache_key(self):
        return self._model_class._gen_cache_key

    def _build_report_miss_msg(self, method_name, *args, **kwargs):
        return 'Miss cache method invocation: `{}.{}({})`'.format(
            self._model_class.__name__, method_name,
            ', '.join(filter(None, (
                ', '.join(map(friendly_repr, list(args))) if args else '',
                ', '.join(
                    '{}={}'.format(k, friendly_repr(v))
                    for k, v in sorted(iteritems(kwargs))
                )
            )))
        )

    def _report_miss(self, method_name, *args, **kwargs):

        if random.random() > 0.02:
            return

        level = kwargs.pop('olo_report_level', logging.WARNING)  # pragma: no cover pylint: disable=E

        msg = self._build_report_miss_msg(  # pragma: no cover
            method_name, *args, **kwargs
        )
        self._model_class._options.report(msg, level=level)  # noqa pragma: no cover

    def get(self, id=None, **kwargs):
        if not kwargs:
            if not self._cache_client:
                return self._model_class._get(id)
            values = self.get_multi([id], filter_none=False)
            return values[0]
        return self.get_by(**kwargs)

    def get_multi(self, idents, filter_none=True):
        def fallback():
            self._report_miss('get_multi', idents, filter_none=filter_none)
            return self._model_class._get_multi(
                idents, filter_none=filter_none
            )

        if not self._cache_client:
            return fallback()

        pk_name = self._model_class.get_singleness_pk_name()

        unique_keys = get_unique_keys(self._model_class)

        # gen_keys
        keys = []
        for ident in idents:
            if not isinstance(ident, dict):
                kwargs = {
                    pk_name: ident
                }
            else:
                if get_str_key(ident) not in unique_keys:
                    raise CacheError(
                        '{} is not a unique key. '
                        'The unique key is {}'.format(
                            repr(tuple(ident)), repr(tuple(unique_keys))
                        )
                    )
                kwargs = ident
            # pylint: disable=E1102
            key = self._gen_cache_key(**kwargs)
            # pylint: enable=E1102
            keys.append(key)

        key_mapping = dict(izip(map(str, idents), keys))
        mapping = self._cache_client.get_multi(keys)

        new_idents = []
        for ident in idents:
            key = key_mapping.get(str(ident))
            value = mapping.get(key)
            if value is None:
                new_idents.append(ident)

        items = self._model_class._get_multi(new_idents, filter_none=False)
        new_mapping = {}
        for item, ident in izip(items, new_idents):
            key = key_mapping.get(str(ident))
            if item is None:
                new_mapping[key] = missing
            else:
                new_mapping[key] = mapping[key] = item._data

        if new_mapping:
            self._cache_client.set_multi(new_mapping)

        session = QuerySession()
        model_class = self._model_class

        for ident in idents:
            key = key_mapping.get(str(ident))
            item = mapping.get(key)

            if isinstance(item, dict):
                item = model_class._olo_instantiate(_olo_decrypt=False, **item)
            else:
                item = None

            if item is None and filter_none:
                continue

            session.add_entity(item)

        self.add_handler(session.entities)

        return session.entities

    gets = get_multi

    @wash_kwargs
    def get_by(self, *args, **kwargs):
        def fallback():
            self._report_miss('get_by', *args, **kwargs)
            return self._model_class._get_by(*args, **kwargs)

        if not self._cache_client or args:
            return fallback()  # pragma: no cover

        str_key = get_str_key(kwargs)
        index_keys = get_index_keys(self._model_class)
        unique_keys = get_unique_keys(self._model_class)

        if str_key not in unique_keys:
            if str_key in index_keys:
                _res = self.get_multi_by(limit=1, **kwargs)
                if _res:
                    return _res[0]
                return
            return fallback()

        # pylint: disable=E1102
        key = self._gen_cache_key(**kwargs)
        # pylint: enable=E1102
        data = self._cache_client.get(key)
        if data is None:
            res = self._model_class._get_by(**kwargs)
            if res is None:
                data = missing
            else:
                data = res._data
            self._cache_client.set(key, data)
        else:
            res = (
                self._model_class._olo_instantiate(_olo_decrypt=False, **data)
                if isinstance(data, dict) else None
            )
        session = QuerySession()
        session.add_entity(res)
        self.add_handler(res)
        return res

    @wash_kwargs
    def get_multi_by(self, *args, **kwargs):
        old_kwargs = dict(kwargs)

        def fallback():
            self._report_miss('get_multi_by', *args, **old_kwargs)
            return self._model_class._get_multi_by(*args, **old_kwargs)

        if not self._cache_client:
            return fallback()

        unique_keys = get_unique_keys(self._model_class)

        start = kwargs.pop('start', 0)
        limit = kwargs.pop('limit', None)
        order_by = kwargs.pop('order_by', None)
        order_by_str = None
        if order_by is not None:
            if isinstance(order_by, list):
                order_by = tuple(order_by)
            elif not isinstance(order_by, tuple):
                order_by = (order_by,)
            order_by_str = order_by_to_strs(order_by)

        if not order_by:
            order_by = None

        str_key = get_str_key(kwargs)
        if str_key in unique_keys:
            inst = self.get_by(**kwargs)
            if inst is None:
                return []
            return [inst]

        index_keys = get_index_keys(self._model_class)
        if (
            args or
            str_key not in index_keys or
            (
                order_by_str and
                order_by_str not in self._model_class.__order_bys__
            )
        ):
            return fallback()

        pk_name = self._model_class.get_singleness_pk_name()

        # pylint: disable=E1102
        key = self._gen_cache_key(_olo_suffix='ids',
                                  order_by=order_by_str, **kwargs)
        # pylint: enable=E1102

        if start is None:
            start = 0  # pragma: no cover
        if limit is None:
            limit = sys.maxsize

        over_limit = start + limit > self.MAX_COUNT

        res = self._cache_client.get(key)
        logger.debug('[CACHE]: get cache by key: %s, value: %s', key, res)
        if res is None:
            res = self._model_class.get_entities_by(
                [pk_name],
                start=0,
                limit=self.MAX_COUNT + 1,
                order_by=order_by,
                **kwargs
            )

            self._cache_client.set(key, res)

        if len(res) == self.MAX_COUNT + 1 and over_limit:
            return fallback()

        return self.gets(res[start: start + limit])

    gets_by = get_multi_by

    @wash_kwargs
    def count_by(self, *expressions, **expression_dict):
        def _get_res():
            return self._model_class._count_by(*expressions, **expression_dict)

        def fallback():
            self._report_miss('count_by', *expressions, **expression_dict)
            return _get_res()

        if not self._cache_client:
            return fallback()

        str_key = get_str_key(expression_dict)
        unique_keys = get_unique_keys(self._model_class)
        index_keys = get_index_keys(self._model_class)
        if (
            expressions or
            (
                str_key not in unique_keys and
                str_key not in index_keys
            )
        ):
            return fallback()

        # pylint: disable=E1102
        key = self._gen_cache_key(_olo_suffix='count', **expression_dict)
        # pylint: enable=E1102
        res = self._cache_client.get(key)
        if res is None:
            res = _get_res()
            self._cache_client.set(key, res)
        return res

    def add_handler(self, insts):
        if not self._db.in_transaction():
            return

        if not isinstance(insts, list):
            insts = [insts]

        insts = filter(lambda inst: isinstance(inst, self._model_class), insts)
        if not insts:
            return  # pragma: no cover

        def _cbk():
            for inst in insts:
                inst._clear_cache()
        self._db.add_rollback_handler(_cbk)


def get_str_key(keys):
    return StrKey(
        k.attr_name if not isinstance(k, str_types) else k
        for k in keys
    )


def get_unique_keys(cls_or_obj):
    keys = set()
    keys.add(cls_or_obj.__primary_key__)
    keys.update(cls_or_obj.__unique_keys__)
    return keys


def get_index_keys(cls_or_obj):
    return cls_or_obj.__index_keys__


def get_cache_keys(obj):
    res = []

    def _add_count_key(**kwargs):
        # pylint: disable=E1102
        res.append(obj._gen_cache_key(_olo_suffix='count', **kwargs))
        # pylint: enable=E1102

    unique_keys = get_unique_keys(obj)
    for key in unique_keys:
        kwargs = {
            attr_name: getattr(obj, attr_name)
            for attr_name in key
        }
        _add_count_key(**kwargs)
        res.append(obj._gen_cache_key(**kwargs))
    index_keys = get_index_keys(obj)
    for key in index_keys:
        kwargs = {
            attr_name: getattr(obj, attr_name)
            for attr_name in key
        }
        _add_count_key(**kwargs)
        res.append(obj._gen_cache_key(
            _olo_suffix='ids', order_by=None, **kwargs
        ))
        for order_by in obj.__order_bys__:
            res.append(obj._gen_cache_key(
                _olo_suffix='ids', order_by=order_by, **kwargs
            ))
    return res


def delete_cache(sender):
    options = sender._options
    if not options.cache_client:
        return
    keys = set(get_cache_keys(sender))
    if sender._orig and sender.__primary_key__:
        dirty_fields = set()

        for name in sender.__fields__:
            if getattr(sender, name) != getattr(sender._orig, name):
                dirty_fields.add(name)

        _keys = set(get_cache_keys(sender._orig))
        inter_keys = keys.intersection(_keys)
        keys = keys | _keys
        if (
            sender._get_singleness_pk_value() ==
            sender._orig._get_singleness_pk_value()
        ):
            _same_ids_keys = {k for k in inter_keys
                             if k.endswith(':suffix:ids')}
            same_ids_keys = set()
            for key in _same_ids_keys:
                for name in dirty_fields:
                    # fix order by modify
                    if "'{}'".format(name) in key or "'-{}'".format(name) in key:
                        break
                else:
                    same_ids_keys.add(key)
            keys -= same_ids_keys
    if keys:
        options.cache_client.delete_multi(list(keys))


def create_cache(sender):
    options = sender._options
    if not options.cache_client:
        return
    keys = get_cache_keys(sender)
    if keys:
        mapping = {key: sender for key in keys}
        options.cache_client.set_multi(mapping,
                                       options.cache_expire)


after_delete.connect(delete_cache)
after_insert.connect(delete_cache)
after_update.connect(delete_cache)
before_update.connect(delete_cache)
