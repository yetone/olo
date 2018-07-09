import re
import sys
import types
import operator

from itertools import chain
from decorator import decorator

from olo.compat import izip, imap, str_types, iteritems, reduce
from olo.interfaces import SQLASTInterface
from olo.field import Field
from olo.errors import ExpressionError, OrderByError, SupportError
from olo.context import table_alias_mapping_context
from olo.libs.compiler.translators.func_translator import transform_func
from olo.session import QuerySession
from olo.utils import optimize_sql_ast


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
        for k, v in iteritems(dct)
    ]


def _process_order_by(model_class, order_by):
    new = []
    for item in order_by:
        if isinstance(item, str_types):
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


def _detect_table_alias(table_section, rev_alias_mapping=None):
    rev_alias_mapping = {} if rev_alias_mapping is None else rev_alias_mapping
    if table_section[0] == 'TABLE':
        alias = table_section[1][0].lower()
        orig_alias = alias
        n = 0
        while alias in rev_alias_mapping:
            n += 1
            alias = orig_alias + str(n)
        rev_alias_mapping[alias] = table_section[1]
        return ['ALIAS', table_section, alias]
    elif isinstance(table_section, list):
        return [_detect_table_alias(x, rev_alias_mapping=rev_alias_mapping)
                for x in table_section]
    return table_section


class Query(SQLASTInterface):

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
            if isinstance(item, str_types):
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
                if start or stop != sys.maxsize:
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
        base_sql_ast, alias_mapping = self._get_base_sql_ast(
            modifier='SQL_CALC_FOUND_ROWS'
        )
        cursor = self.db.get_cursor()
        rv = self._get_rv(base_sql_ast=base_sql_ast,
                          alias_mapping=alias_mapping,
                          cursor=cursor)
        cursor.execute('SELECT FOUND_ROWS()')
        count = cursor.fetchone()[0]
        items = list(self._iter_wrap_rv(rv))
        return count, items

    __len__ = count

    def update(self, **values):
        expression = self._get_expression()
        if not expression:
            raise ExpressionError('Cannot execute update because of '
                                  'without expression')
        expressions, _, _ = self._model_class._split_attrs(values)

        sql_ast = [
            'UPDATE',
            ['TABLE', self.table_name],
            ['SET',
             ['SERIES'] + [exp.get_sql_ast() for exp in expressions]]
        ]
        rows = self._get_rv(base_sql_ast=sql_ast)
        if self.db.autocommit:
            self.db.commit()
        return rows

    def delete(self):
        expression = self._get_expression()
        if not expression:
            raise ExpressionError('Cannot execute delete because of '
                                  'without expression')
        sql_ast = [
            'DELETE',
            ['TABLE', self.table_name]
        ]
        rows = self._get_rv(base_sql_ast=sql_ast)
        if self.db.autocommit:
            self.db.commit()
        return rows

    @property
    def table_name(self):
        return self._model_class._get_table_name()

    def _get_rv(self, base_sql_ast=None,
                alias_mapping=None,
                cursor=None):
        return self.__get_rv(
            base_sql_ast=base_sql_ast,
            alias_mapping=alias_mapping,
            cursor=cursor,
        )

    def __get_rv(self, base_sql_ast=None,
                 alias_mapping=None,
                 cursor=None):
        sql_ast = self.get_sql_ast(
            base_sql_ast=base_sql_ast,
            alias_mapping=alias_mapping,
        )

        sql, params = self.db.ast_translator.translate(sql_ast)

        if cursor is not None:
            cursor.execute(sql, params)
            return cursor.fetchall()

        return self.db.execute(sql, params)

    def get_sql_ast(self, base_sql_ast=None, alias_mapping=None):
        sql_ast = self.get_primitive_sql_ast(
            base_sql_ast=base_sql_ast, alias_mapping=alias_mapping)
        return optimize_sql_ast(sql_ast)

    def get_primitive_sql_ast(self, base_sql_ast=None, alias_mapping=None):
        if base_sql_ast is None:
            base_sql_ast, alias_mapping = self._get_base_sql_ast()
        alias_mapping = alias_mapping or {}
        with table_alias_mapping_context(alias_mapping):
            return self._get_primitive_sql_ast(base_sql_ast)

    def _get_primitive_sql_ast(self, base_sql_ast):
        sql_ast = [x for x in base_sql_ast]
        if self._on_expressions:
            sql_ast.append([
                'ON',
                ['AND'] + [e.get_sql_ast() for e in self._on_expressions]
            ])
        if self._expressions:
            sql_ast.append([
                'WHERE',
                ['AND'] + [e.get_sql_ast() for e in self._expressions]
            ])

        if self._group_by:
            fields = self._get_fields(self._group_by)
            sql_ast.append([
                'GROUP BY',
                ['SERIES'] + [f.get_sql_ast() for f in fields]
            ])

        if self._having_expressions:
            sql_ast.append([
                'HAVING',
                ['AND'] + [e.get_sql_ast() for e in self._having_expressions]
            ])

        if self._order_by:
            fields = self._get_fields(self._order_by)
            sql_ast.append([
                'ORDER BY',
                ['SERIES'] + [f.get_sql_ast() for f in fields]
            ])

        if self._limit is not None:
            limit_section = ['LIMIT', None, ['VALUE', self._limit]]

            if self._offset is not None and self._offset != 0:
                limit_section[1] = ['VALUE', self._offset]

            sql_ast.append(limit_section)

        return sql_ast

    def _get_expression(self, is_having=False, is_on=False):
        if is_having:
            expressions = self._having_expressions  # pragma: no cover
        elif is_on:
            expressions = self._on_expressions  # pragma: no cover
        else:
            expressions = self._expressions

        if expressions:
            return reduce(operator.and_, expressions)

    def _get_base_sql_ast(self, modifier=None, entities=None):
        entities = self._entities if entities is None else entities

        if self._join:
            table_section = [
                'JOIN',
                ['TABLE', self.table_name],
                ['TABLE', self._join._get_table_name()]
            ]
        elif self._left_join:
            table_section = [
                'LEFT JOIN',
                ['TABLE', self.table_name],
                ['TABLE', self._left_join._get_table_name()]
            ]
        elif self._right_join:
            table_section = [
                'RIGHT JOIN',
                ['TABLE', self.table_name],
                ['TABLE', self._right_join._get_table_name()]
            ]
        else:
            table_section = ['TABLE', self.table_name]

        rev_alias_mapping = {}
        table_section = _detect_table_alias(
            table_section,
            rev_alias_mapping=rev_alias_mapping
        )

        alias_mapping = {v: k for k, v in iteritems(rev_alias_mapping)}

        with table_alias_mapping_context(alias_mapping):
            select_ast = [
                'SERIES',
            ] + [e.get_sql_ast() if hasattr(e, 'get_sql_ast') else e
                 for e in entities]
            if len(select_ast) == 2 and select_ast[1][0] == 'SERIES':
                select_ast = select_ast[1]
            if modifier is not None:
                select_ast = ['MODIFIER', modifier, select_ast]

        sql_ast = ['SELECT']
        sql_ast.append(select_ast)
        sql_ast.append(['FROM', table_section])
        return sql_ast, alias_mapping

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
                        izip(
                            v.__sorted_fields__,
                            item[idx: idx + fields_count]
                        )  # pylint: disable=W
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

    def _get_fields(self, fields):
        if not isinstance(fields, (list, tuple)):
            fields = [fields]  # pragma: no cover

        res = []

        for field in fields:
            if isinstance(field, str_types):
                _field = getattr(self._model_class, field, None)
                if not _field:
                    raise ExpressionError('Cannot find field: `{}`'.format(  # noqa pragma: no cover pylint: disable=W
                        field
                    ))
                field = _field
            res.append(field)
        return res
