from __future__ import annotations

import operator
import re
import sys
import types
from enum import Enum
from typing import TYPE_CHECKING, Optional, List, Union, Iterable, Type, Tuple

from olo.expression import UnaryExpression, BinaryExpression, Expression
from olo.funcs import DISTINCT, Function

if TYPE_CHECKING:
    from olo.database import OLOCursor
    from olo.model import Model, ModelMeta

from itertools import chain
from decorator import decorator

from olo.compat import izip, imap, str_types, iteritems, reduce
from olo.interfaces import SQLASTInterface
from olo.field import Field
from olo.errors import ExpressionError, OrderByError, SupportError, ORMError
from olo.libs.compiler.translators.func_translator import transform_func
from olo.session import QuerySession
from olo.utils import optimize_sql_ast, friendly_repr


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


def _process_order_by(model_class, order_by) -> List[UnaryExpression]:
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
                raise OrderByError('`{}` is an invalid order_by'.format(  # noqa pragma: no cover pylint: disable=W
                    item
                ))
            item = f
            if is_negative:
                item = item.desc()
            else:
                item = item.asc()
        elif isinstance(item, Field):
            item = item.asc()
        else:
            if not isinstance(item, UnaryExpression):
                raise OrderByError('`{}` is an invalid order_by'.format(  # noqa pragma: no cover pylint: disable=W
                    item
                ))
        new.append(item)
    return new


@decorator
def _lambda_eval(func, self, *args, **kwargs):
    if len(args) == 1 and isinstance(args[0], types.FunctionType):
        lamb = transform_func(args[0])
        return func(self, lamb(self._model_class), **kwargs)
    return func(self, *args, **kwargs)


def _check_aggregation(exp: Expression) -> bool:
    if isinstance(exp, BinaryExpression):
        if isinstance(exp.left, Function):
            return True
        if isinstance(exp.right, Function):
            return True
        if isinstance(exp.left, Expression) and _check_aggregation(exp.left):
                return True
        if isinstance(exp.right, Expression) and _check_aggregation(exp.right):
                return True
    return False


def _split_where_expression_and_having_expression(expression: BinaryExpression) -> Tuple[Optional[BinaryExpression],
                                                                                         Optional[BinaryExpression]]:
    stack = [expression]
    and_expressions = []
    while stack:
        exp = stack.pop()
        if exp is None:
            continue
        if exp.operator == 'AND':
            stack.append(exp.right)
            stack.append(exp.left)
            continue
        and_expressions.append(exp)
    where_expressions = []
    having_expressions = []
    for exp in and_expressions:
        if _check_aggregation(exp):
            having_expressions.append(exp)
        else:
            where_expressions.append(exp)
    where_expression = None
    having_expression = None
    if where_expressions:
        where_expression = reduce(operator.and_, where_expressions)
    if having_expressions:
        having_expression = reduce(operator.and_, having_expressions)
    return where_expression, having_expression


class JoinType(Enum):
    INNER = 0
    LEFT = 1
    RIGHT = 2
    FULL = 3


class JoinChain(SQLASTInterface):
    on_: Optional[BinaryExpression]

    def __init__(self, type_: JoinType, left: Union[Model, JoinChain], right: Model) -> None:
        self.type = type_
        self.left = left
        self.right = right
        self.on_ = None

    def on(self, on: BinaryExpression) -> None:
        if self.on_ is None:
            self.on_ = on
            return
        self.on_ = self.on_ & on

    def get_sql_ast(self) -> List:
        from olo.model import Model
        if isinstance(self.left, type) and issubclass(self.left, Model):
            left_ast = ['TABLE', self.left._get_table_name()]
        else:
            left_ast = self.left.get_sql_ast()
        on_ast = []
        if self.on_:
            on_ast = self.on_.get_sql_ast()
        return ['JOIN', self.type.name, left_ast, ['TABLE', self.right._get_table_name()], on_ast]

    def clone(self) -> JoinChain:
        cloned = JoinChain(self.type, self.left, self.right)
        cloned.on(self.on_)
        return cloned


if TYPE_CHECKING:
    Entity = Union[Type[Model], Field, Function]


class Query(SQLASTInterface):

    def __init__(self, model_class: Type[Model]):
        self._model_class = model_class
        self._expression: Optional[BinaryExpression] = None
        self._having_expression: Optional[BinaryExpression] = None
        self._offset = 0
        self._limit = None
        self._order_by: List[UnaryExpression] = []
        self._group_by = []
        self._entities: List[Entity] = [model_class]
        self._raw = False
        self._join_chain: Optional[JoinChain] = None
        self._for_update = False

    def _update(self, **kwargs):
        inst = self.__class__(self._model_class)
        inst.__dict__.update(self.__dict__)
        inst.__dict__.update(kwargs)
        return inst

    def _get_entities(self, fields: Iterable[Union[Entity, str]]) -> List[Entity]:
        from olo.model import ModelMeta

        if not isinstance(fields, (list, tuple, set)):
            fields = [fields]

        res = []
        for field in fields:
            if isinstance(field, str_types):
                field_ = self._model_class._olo_get_field(field)
                if field_ is None:
                    raise ORMError(f'{friendly_repr(field)} is not a valid field in Model {self._model_class.__name__}')
                field = field_
            if not isinstance(field, (ModelMeta, Field, Function)):
                raise ORMError(f'{field} is an not valid entity!')
            res.append(field)
        return res

    @_lambda_eval
    def map(self, *entities: Union[Entity, str], **kwargs):
        from olo.model import ModelMeta

        self._raw = kwargs.get('raw', False)
        entities = self._get_entities(entities)

        q = self._update(_entities=list(
            chain.from_iterable(
                x if isinstance(x, (list, tuple, set)) else (x,)
                for x in entities
            )
        ))

        has_aggregation = False
        first_field = None
        for entity in q._entities:
            if isinstance(entity, ModelMeta) and first_field is None:
                first_field = self._model_class.get_singleness_pk_field()

            if isinstance(entity, Field) and first_field is None:
                first_field = entity

            if isinstance(entity, Function):
                has_aggregation = True

        if has_aggregation and first_field is not None:
            q = q.group_by(first_field)

        return q

    def __call__(self, *entities, **kwargs):
        return self.map(*entities, **kwargs)

    @_lambda_eval
    def flat_map(self, query):
        return self.join(query._model_class).on(
            query._expression
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
        left = self._model_class
        if self._join_chain is not None:
            left = self._join_chain
        return self._update(_join_chain=JoinChain(JoinType.INNER, left, model_class))

    @_lambda_eval
    def left_join(self, model_class):
        left = self._model_class
        if self._join_chain is not None:
            left = self._join_chain
        return self._update(_join_chain=JoinChain(JoinType.LEFT, left, model_class))

    @_lambda_eval
    def right_join(self, model_class):
        left = self._model_class
        if self._join_chain is not None:
            left = self._join_chain
        return self._update(_join_chain=JoinChain(JoinType.RIGHT, left, model_class))

    @_lambda_eval
    def full_join(self, model_class):
        left = self._model_class
        if self._join_chain is not None:
            left = self._join_chain
        return self._update(_join_chain=JoinChain(JoinType.FULL, left, model_class))

    @_lambda_eval
    def filter(self, *expressions, **expression_dict):
        self._model_class._check_attrs(expression_dict)
        expression_dict = self._model_class._wash_attrs(
            expression_dict
        )

        expressions = list(expressions) + list(
            _dict_to_expressions(
                self._model_class, expression_dict
            )
        )

        expression = self._expression

        if expressions:
            _expression = reduce(
                operator.and_,
                expressions,
            )
            if expression is not None:
                expression &= _expression
            else:
                expression = _expression

        expression, having_expression = _split_where_expression_and_having_expression(expression)

        q = self
        if expression is not None:
            q = q._update(_expression=expression)

        if having_expression is not None:
            q = q.having(having_expression)

        return q

    @_lambda_eval
    def on(self, *on_expressions, **on_expression_dict):
        if self._join_chain is None:
            raise ORMError('this query does not have a join chain!')

        self._model_class._check_attrs(on_expression_dict)
        on_expression_dict = self._model_class._wash_attrs(
            on_expression_dict
        )
        on_expressions = list(on_expressions) + list(
            _dict_to_expressions(
                self._model_class, on_expression_dict
            )
        )
        on_expression = reduce(
            operator.and_,
            on_expressions
        )
        join_chain = self._join_chain.clone()
        join_chain.on(on_expression)
        return self._update(_join_chain=join_chain)

    @_lambda_eval
    def having(self, *having_expressions, **having_expression_dict):
        self._model_class._check_attrs(having_expression_dict)
        having_expression_dict = self._model_class._wash_attrs(
            having_expression_dict
        )
        having_expressions = (
            list(having_expressions) + list(
                _dict_to_expressions(
                    self._model_class, having_expression_dict
                )
            )
        )
        q = self
        if having_expressions:
            having_expression = reduce(operator.and_, having_expressions)
            if self._having_expression is not None:
                having_expression = self._having_expression & having_expression
            q = q._update(_having_expression=having_expression)
        return q

    def for_update(self):
        return self._update(_for_update=True)

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
        base_sql_ast = self._get_base_sql_ast(
            modifier='SQL_CALC_FOUND_ROWS'
        )
        with self.db.transaction():
            cursor = self.db.get_cursor()
            rv = self._get_rv(base_sql_ast=base_sql_ast,
                              cursor=cursor)
            cursor.ast_execute(['SELECT', ['CALL', 'FOUND_ROWS']])
            count = cursor.fetchone()[0]
        items = list(self._iter_wrap_rv(rv))
        return count, items

    __len__ = count

    def update(self, **values):
        from olo import PostgreSQLDataBase

        expression = self._get_expression()
        if not expression:
            raise ExpressionError('Cannot execute update because of '
                                  'without expression')

        assignments, _, _ = self._model_class._split_attrs(values)

        update_sql_ast = [
            'UPDATE',
            ['TABLE', self.table_name],
            ['SET',
             ['SERIES'] + [asg.get_sql_ast() for asg in assignments]],
        ]

        db = self._model_class._get_db()

        # FIXME(PG)
        if isinstance(db, PostgreSQLDataBase):
            pk = self._model_class.get_singleness_pk_field()
            if self._order_by:
                base_sql_ast = self.map(pk).for_update()._get_base_sql_ast()
                sql_ast = self.get_sql_ast(
                    base_sql_ast=base_sql_ast,
                )
                update_sql_ast.append(
                    ['WHERE', ['BINARY_OPERATE', 'IN', ['QUOTE', pk.name], ['BRACKET', sql_ast]]]
                )
                with self.db.transaction():
                    rows = self._get_rv_by_sql_ast(sql_ast=update_sql_ast)
            else:
                with self.db.transaction():
                    rows = self._get_rv(base_sql_ast=update_sql_ast)
        else:
            with self.db.transaction():
                rows = self._get_rv(base_sql_ast=update_sql_ast)
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
        with self.db.transaction():
            rows = self._get_rv(base_sql_ast=sql_ast)
        return rows

    @property
    def table_name(self):
        return self._model_class._get_table_name()

    def _get_rv(self, base_sql_ast=None,
                cursor=None):
        return self.__get_rv(
            base_sql_ast=base_sql_ast,
            cursor=cursor,
        )

    def __get_rv(self, base_sql_ast=None,
                 cursor=None):
        sql_ast = self.get_sql_ast(
            base_sql_ast=base_sql_ast,
        )
        return self._get_rv_by_sql_ast(sql_ast, cursor=cursor)

    def _get_rv_by_sql_ast(self, sql_ast, cursor: Optional[OLOCursor] = None):
        if cursor is not None:
            cursor.ast_execute(sql_ast)
            return cursor.fetchall()

        with self.db.transaction():
            return self.db.ast_execute(sql_ast)

    def get_sql_ast(self, base_sql_ast=None):
        sql_ast = self.get_primitive_sql_ast(
            base_sql_ast=base_sql_ast)
        return optimize_sql_ast(sql_ast)

    def get_primitive_sql_ast(self, base_sql_ast=None):
        if base_sql_ast is None:
            base_sql_ast = self._get_base_sql_ast()
        return self._get_primitive_sql_ast(base_sql_ast)

    def _entities_contains(self, field):
        if len(self._entities) == 1 and self._entities[0] is self._model_class:
            return True
        for f in self._entities:
            # f == field will return an Expression object, so must compare with True explicitly
            if (f == field) is True:
                return True
            if getattr(f, 'name', 'f.name') == getattr(field, 'name', 'field.name'):
                return True
        return False

    def _get_primitive_sql_ast(self, base_sql_ast):
        sql_ast = list(base_sql_ast)  # copy ast
        if self._expression is not None:
            sql_ast.append([
                'WHERE',
                self._expression.get_sql_ast()
            ])

        if self._having_expression is not None and not self._group_by:
            group_by = []
            for entity in self._entities:
                if entity is self._model_class:
                    pk = self._model_class.get_singleness_pk_field()
                    group_by.append(pk)
                    break

                if isinstance(entity, Field):
                    group_by.append(entity)

            self._group_by = group_by

        if self._group_by:
            entities = self._get_entities(self._group_by)
            field_names = {getattr(f, 'name', '') for f in entities}
            pk = self._model_class.get_singleness_pk_field()
            # self._entities must casting to set or pk in self._entities will always be True!!!
            if self._entities_contains(pk) and pk.name not in field_names:
                entities.append(pk)
            sql_ast.append([
                'GROUP BY',
                ['SERIES'] + [f.get_sql_ast() for f in entities]
            ])

        if self._having_expression is not None:
            sql_ast.append([
                'HAVING',
                self._having_expression.get_sql_ast()
            ])

        if self._order_by:
            sql_ast.append([
                'ORDER BY',
                ['SERIES'] + [f.get_sql_ast() for f in self._order_by]
            ])

        if self._limit is not None:
            limit_section = ['LIMIT', None, ['VALUE', self._limit]]

            if self._offset is not None and self._offset != 0:
                limit_section[1] = ['VALUE', self._offset]

            sql_ast.append(limit_section)

        if self._for_update:
            sql_ast.append(['FOR UPDATE'])

        return sql_ast

    def _get_expression(self, is_having=False):
        if is_having:
            return self._having_expression
        return self._expression

    def _get_base_sql_ast(self, modifier=None, entities=None):
        entities = self._entities if entities is None else entities

        if self._join_chain:
            table_section = self._join_chain.get_sql_ast()
        else:
            table_section = ['TABLE', self.table_name]

        contains_distinct = False
        for entity in entities:
            if isinstance(entity, DISTINCT):
                contains_distinct = True
                break

        # FIXME(PG)
        if contains_distinct and self._order_by:
            for ob in self._order_by:
                if not self._entities_contains(ob.value):
                    entities = entities + [ob.value]

        select_ast = [
            'SERIES',
        ] + [e.get_sql_ast() if hasattr(e, 'get_sql_ast') else e
             for e in entities]
        if len(select_ast) == 2 and select_ast[1][0] == 'SERIES':
            select_ast = select_ast[1]
        if modifier is not None:
            select_ast = ['MODIFIER', modifier, select_ast]

        sql_ast = ['SELECT', select_ast, ['FROM', table_section]]
        return sql_ast

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

        seen = set()

        for idx, item in enumerate(rv):
            new_item = tuple(imap(lambda f: f(item), producers))  # noqa pylint: disable=W
            new_item = new_item[:entity_count]

            if entity_count == 1:
                new_item = new_item[0]
                # TODO
                if isinstance(self._entities[0], DISTINCT):
                    if new_item in seen:
                        continue
                    seen.add(new_item)

            session.add_entity(new_item)

        for entity in session.entities:
            yield entity
