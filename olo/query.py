import re
import sys
import types
import operator

from itertools import izip, imap, chain
from decorator import decorator

from olo.interfaces import SQLLiteralInterface
from olo.field import Field
from olo.errors import ExpressionError, OrderByError, SupportError
from olo.context import field_verbose_context
from olo.libs.compiler.translators.func_translator import transform_func
from olo.session import QuerySession
from olo.utils import (
    sql_and_params, get_sql_pieces_and_params
)


PATTERN_NEG = re.compile(r'^\-')
PATTERN_BACKQUOTE = re.compile('^`(?P<name>.*)`$')


def _strip_backquote(s):
    m = PATTERN_BACKQUOTE.search(s)
    if not m:
        return s
    return m.group('name')  # pragma: no cover


def _dict_to_expressions(model_class, dct):
    return [
        getattr(model_class, k) == v
        for k, v in dct.iteritems()
    ]


def _process_order_by(model_class, order_by):
    new = []
    for item in order_by:
        if isinstance(item, basestring):
            _item = item
            _item = _strip_backquote(_item)
            is_negative = bool(PATTERN_NEG.search(_item))
            if is_negative:
                _item = PATTERN_NEG.sub('', _item)
            else:
                _item, _, sort = _item.partition(' ')
                is_negative = sort.lower() == 'desc'
                if sort:
                    _item = _strip_backquote(_item)
            f = getattr(model_class, _item, None)
            if f is None:
                raise OrderByError('`{}` is a valid order_by'.format(  # noqa pragma: no cover pylint: disable=W
                    item
                ))
            item = f
            if is_negative:
                item = item.desc()
            else:
                item = item.asc()
        new.append(item)
    return new


@decorator
def _lambda_eval(func, self, *args, **kwargs):
    if len(args) == 1 and isinstance(args[0], types.FunctionType):
        lamb = transform_func(args[0])
        return func(self, lamb(self._model_class), **kwargs)
    return func(self, *args, **kwargs)


class Query(SQLLiteralInterface):

    def __init__(self, model_class):
        self._model_class = model_class
        self._expressions = []
        self._having_expressions = []
        self._on_expressions = []
        self._offset = 0
        self._limit = None
        self._order_by = []
        self._group_by = []
        self._entities = [model_class]
        self._raw = False
        self._join = None
        self._left_join = None
        self._right_join = None

    def _update(self, **kwargs):
        inst = self.__class__(self._model_class)
        inst.__dict__.update(self.__dict__)
        inst.__dict__.update(kwargs)
        return inst

    def _transform_entities(self, entities):
        res = []
        for item in entities:
            if isinstance(item, basestring):
                field = self._model_class._olo_get_field(item)
                if field is not None:
                    item = field
            res.append(item)
        return res

    @_lambda_eval
    def map(self, *entities, **kwargs):
        self._raw = kwargs.get('raw', False)
        entities = self._transform_entities(entities)
        return self._update(_entities=list(
            chain.from_iterable(
                x if isinstance(x, (list, tuple, set)) else (x,)
                for x in entities
            )
        ))

    def __call__(self, *entities, **kwargs):
        return self.map(*entities, **kwargs)

    @_lambda_eval
    def flat_map(self, query):
        return self.join(query._model_class).filter(
            *query._expressions
        ).map(*query._entities)

    def __getitem__(self, item):
        if isinstance(item, slice):
            q = self
            start = item.start or 0
            stop = item.stop
            step = item.step
            if step is not None:
                raise SupportError(
                    'Cannot support step in __getitem__ now!'
                )
            if start:
                q = q.offset(start)
            if stop is not None:
                if start or stop != sys.maxint:
                    q = q.limit(stop - start)
            return q.all()
        field = self._model_class.get_singleness_pk_field()
        return self.filter(field == item).first()

    @property
    def db(self):
        return self._model_class._get_db()

    @property
    def cq(self):
        return self

    query = cq

    @_lambda_eval
    def join(self, model_class):
        return self._update(_join=model_class)

    @_lambda_eval
    def left_join(self, model_class):
        return self._update(_left_join=model_class)

    @_lambda_eval
    def right_join(self, model_class):
        return self._update(_right_join=model_class)

    @_lambda_eval
    def filter(self, *expressions, **expression_dict):
        self._model_class._check_attrs(expression_dict)
        expression_dict = self._model_class._wash_attrs(
            expression_dict
        )
        expressions = self._expressions + list(expressions) + list(
            _dict_to_expressions(
                self._model_class, expression_dict
            )
        )
        return self._update(_expressions=expressions)

    @_lambda_eval
    def on(self, *on_expressions, **on_expression_dict):
        self._model_class._check_attrs(on_expression_dict)
        on_expression_dict = self._model_class._wash_attrs(
            on_expression_dict
        )
        on_expressions = self._on_expressions + list(on_expressions) + list(
            _dict_to_expressions(
                self._model_class, on_expression_dict
            )
        )
        return self._update(_on_expressions=on_expressions)

    @_lambda_eval
    def having(self, *having_expressions, **having_expression_dict):
        self._model_class._check_attrs(having_expression_dict)
        having_expression_dict = self._model_class._wash_attrs(
            having_expression_dict
        )
        having_expressions = (
            self._having_expressions + list(having_expressions) + list(
                _dict_to_expressions(
                    self._model_class, having_expression_dict
                )
            )
        )
        return self._update(_having_expressions=having_expressions)

    def offset(self, offset):
        return self._update(_offset=offset)

    def limit(self, limit):
        return self._update(_limit=limit)

    def order_by(self, *order_by):
        order_by = _process_order_by(self._model_class, order_by)
        _order_by = self._order_by + list(order_by)
        return self._update(_order_by=_order_by)

    def group_by(self, *group_by):
        _group_by = self._group_by + list(group_by)
        return self._update(_group_by=_group_by)

    def first(self):
        res = self.limit(1).all()
        return res[0] if res else None

    one = first

    def __iter__(self):
        rv = self._get_rv()
        return self._iter_wrap_rv(rv)

    def all(self):
        return list(self.__iter__())

    def count(self):
        from olo.funcs import COUNT
        return COUNT(self).first()  # pylint: disable=E1101

    def count_and_all(self):
        base_sql, base_params = self._get_base_sql_and_params(
            modifiers='SQL_CALC_FOUND_ROWS'
        )
        cursor = self.db.get_cursor()
        rv = self._get_rv(base_sql=base_sql, base_params=base_params,
                          cursor=cursor)
        cursor.execute('SELECT FOUND_ROWS()')
        count = cursor.fetchone()[0]
        items = list(self._iter_wrap_rv(rv))
        return count, items

    __len__ = count

    def update(self, **values):
        expression, _ = self._get_expression_and_params()
        if not expression:
            raise ExpressionError('Cannot execute update because of '
                                  'without expression')
        expressions, _, _ = self._model_class._split_attrs(values)

        sql_pieces, params = get_sql_pieces_and_params(expressions)

        sql = 'UPDATE `{table}` SET {columns} '.format(
            table=self.table_name,
            columns=', '.join(sql_pieces),
        )
        rows = self._get_rv(base_sql=sql, base_params=params)
        if self.db.autocommit:
            self.db.commit()
        return rows

    def delete(self):
        expression, _ = self._get_expression_and_params()
        if not expression:
            raise ExpressionError('Cannot execute delete because of '
                                  'without expression')
        sql = 'DELETE FROM `{table}` '.format(
            table=self.table_name,
        )
        rows = self._get_rv(base_sql=sql)
        if self.db.autocommit:
            self.db.commit()
        return rows

    @property
    def table_name(self):
        return self._model_class._get_table_name()

    def _get_rv(self, base_sql=None, base_params=None,
                cursor=None):
        return self.__get_rv(
            base_sql=base_sql,
            base_params=base_params,
            cursor=cursor,
        )

    def __get_rv(self, base_sql=None, base_params=None,
                 cursor=None):
        sql, params = self.get_sql_and_params(
            base_sql=base_sql, base_params=base_params
        )

        if cursor is not None:
            cursor.execute(sql, params)
            return cursor.fetchall()

        return self.db.execute(sql, params)

    def get_sql_and_params(self, base_sql=None, base_params=None):  # pylint: disable=W
        # pylint: disable=E1121,E1129
        with field_verbose_context(
            bool(self._join or self._left_join or self._right_join)
        ):
            # pylint: enable=E1121,E1129
            return self._get_sql_and_params(
                base_sql=base_sql, base_params=base_params,
            )

    def _get_sql_and_params(self, base_sql=None, base_params=None):
        base_params = base_params or []
        start = self._offset
        order_by = self._order_by
        group_by = self._group_by
        limit = self._limit

        params = []

        expression, where_params = self._get_expression_and_params()
        having_expression, having_params = self._get_expression_and_params(
            is_having=True
        )
        on_expression, on_params = self._get_expression_and_params(
            is_on=True
        )

        if base_sql is None:
            base_sql, _base_params = self._get_base_sql_and_params()

            base_params = base_params + _base_params

        params.extend(base_params)

        sql = base_sql

        if on_expression:
            sql += 'ON {expression} '.format(expression=on_expression)
            params.extend(on_params)

        if expression:
            sql += 'WHERE {expression} '.format(expression=expression)
            params.extend(where_params)

        if group_by:
            strs, _params = self._get_field_strs_and_params(group_by)
            if strs:
                sql += 'GROUP BY {group_by} '.format(
                    group_by=', '.join(strs)
                )
                params.extend(_params)

        if having_expression:
            sql += 'HAVING {expression} '.format(expression=having_expression)
            params.extend(having_params)

        if order_by:
            strs, _params = self._get_field_strs_and_params(order_by)
            if strs:
                sql += 'ORDER BY {order_by} '.format(
                    order_by=', '.join(strs)
                )
                params.extend(_params)

        if limit is not None:
            sql += 'LIMIT {limit} '.format(limit=limit)

            if start:
                sql += 'OFFSET {start} '.format(start=start)

        return sql, params

    def _get_expression_and_params(self, is_having=False, is_on=False):
        if is_having:
            expressions = self._having_expressions
        elif is_on:
            expressions = self._on_expressions
        else:
            expressions = self._expressions

        if expressions:
            expression = reduce(operator.and_, expressions)

            return sql_and_params(expression)

        return '', []

    def _get_columns_str_and_params(self):
        sql_pieces, params = get_sql_pieces_and_params(self._entities)
        columns_str = ', '.join(sql_pieces)
        return columns_str, params

    def _get_base_sql_and_params(self, modifiers=None, select_expr=None,
                                 select_params=None):
        select_params = select_params or []
        if self._join:
            table_name = '`%s` JOIN `%s`' % (
                self.table_name, self._join._get_table_name()
            )
        elif self._left_join:
            table_name = '`%s` LEFT JOIN `%s`' % (
                self.table_name, self._left_join._get_table_name()
            )
        elif self._right_join:
            table_name = '`%s` RIGHT JOIN `%s`' % (
                self.table_name, self._right_join._get_table_name()
            )
        else:
            table_name = '`%s`' % self.table_name

        if modifiers:
            prefix = '{} '.format(modifiers)
        else:
            prefix = ''

        if select_expr is None:
            select_expr, _select_params = self._get_columns_str_and_params()
            select_params = select_params + _select_params

        return 'SELECT {prefix}{select_expr} FROM {table_name} '.format(
            prefix=prefix,
            select_expr=select_expr,
            table_name=table_name
        ), select_params

    # pylint: disable=E0602
    def _iter_wrap_rv(self, rv):
        from olo.model import ModelMeta

        entity_count = len(self._entities)
        raw = self._raw

        producers = []
        idx = -1

        def make_field_producer(idx, v):

            def producer(item):
                if raw:
                    return item[idx]
                model = v.get_model()
                attrs = model._parse_attrs({
                    v.attr_name: item[idx]
                })
                return attrs[v.attr_name]

            return producer

        for v in self._entities:
            idx += 1

            if isinstance(v, ModelMeta):
                fields_count = len(v.__fields__)
                producers.append((
                    lambda idx, v:
                    lambda item: v._olo_instantiate(**dict(
                        izip(v.__fields__, item[idx: idx + fields_count])  # pylint: disable=W
                    ))
                )(idx, v))
                idx += fields_count - 1
                continue

            if isinstance(v, Field):
                producers.append(make_field_producer(idx, v))
                continue

            producers.append((
                lambda idx, v:
                lambda item: item[idx]
            )(idx, v))

        session = QuerySession()

        for idx, item in enumerate(rv):
            new_item = tuple(imap(lambda f: f(item), producers))  # pylint: disable=W
            if entity_count == 1:
                new_item = new_item[0]

            session.add_entity(new_item)

        for entity in session.entities:
            yield entity

    def _get_field_strs_and_params(self, fields):
        if not isinstance(fields, (list, tuple)):
            fields = [fields]  # pragma: no cover

        strs = []
        params = []

        for field in fields:
            if isinstance(field, basestring):
                _field = getattr(self._model_class, field, None)
                if not _field:
                    raise ExpressionError('Cannot find field: `{}`'.format(  # noqa pragma: no cover pylint: disable=W
                        field
                    ))
                field = _field

            sql, _params = sql_and_params(field)
            if _params:
                params.extend(_params)

            alias_name = getattr(field, 'alias_name', None)
            if alias_name:
                strs.append(alias_name)
            else:
                strs.append(sql)
        return strs, params
