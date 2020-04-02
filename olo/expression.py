from __future__ import annotations

from abc import ABC
from enum import Enum
from functools import wraps
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from olo.field import Field, ConstField

from olo.compat import long, str_types
from olo.interfaces import SQLASTInterface
from olo.mixins.operations import BinaryOperationMixin
from olo.utils import get_neg_operator


__all__ = ['UnaryExpression', 'BinaryExpression']


def _auto_transform_to_bin_exp(func):
    @wraps(func)
    def wrapper(self, other):
        other = transform_to_bin_exp(other)
        return func(self, other)
    return wrapper


class Expression(SQLASTInterface, ABC):
    def __neg__(self):
        return  # pragma: no cover


class UnaryExpression(Expression):

    def __init__(self, value: Union[BinaryExpression, Field], operator: str, suffix=True) -> None:
        self.value = value
        self.operator = operator
        self.suffix = suffix

    def get_sql_ast(self):
        return [
            'UNARY_OPERATE',
            self.value.get_sql_ast(),
            self.operator,
            self.suffix
        ]

    def __neg__(self):
        op = get_neg_operator(self.operator, is_unary=True)  # pragma: no cover
        if not op:  # pragma: no cover
            return  # pragma: no cover
        return self.__class__(self.value, op)  # pragma: no cover


def transform_to_bin_exp(item: Union[BinaryExpression, bool, int, long, float, str]) -> Union[BinaryExpression,
                                                                                              ConstField]:
    from olo.field import ConstField
    from olo import funcs

    if isinstance(item, BinaryExpression):
        return item

    if isinstance(item, bool):
        return ConstField(int(item))

    field = ConstField(item)

    if isinstance(item, (int, long, float)):
        return field != 0

    if isinstance(item, str_types):
        return funcs.LENGTH(field) > 0

    raise TypeError(
        'Cannot transform the value to BinaryExpression: {}'.format(
            repr(item)
        )
    )


class BinaryExpression(Expression, BinaryOperationMixin):
    def __init__(self, left, right, operator):
        self.left = left
        self.right = right
        self.operator = operator
        if isinstance(self.right, Enum):
            self.right = self.right.name
        elif isinstance(self.right, type) and issubclass(self.right, Enum):
            self.right = tuple(self.right.__members__)

    @_auto_transform_to_bin_exp
    def __and__(self, other):
        return self.__class__(self, other, 'AND')

    @_auto_transform_to_bin_exp
    def __or__(self, other):
        return self.__class__(self, other, 'OR')

    def __repr__(self):
        return (
            'BinaryExpression(left={left}, right={right}, operator={operator})'
        ).format(
            left=repr(self.left),
            right=repr(self.right),
            operator=repr(self.operator)
        )

    def get_sql_ast(self):
        if isinstance(self.left, Expression) and self.operator == '=':
            """Optimize:
            `(a IN (1, 2)) = True` => `a IN (1, 2)`
            `(a IN (1, 2)) = False` => `a NOT IN (1, 2)`
            """
            if self.right is True:
                return self.left.get_sql_ast()
            if self.right is False:
                nleft = -self.left
                if nleft:
                    return nleft.get_sql_ast()

        sql_ast = ['BINARY_OPERATE', self.operator]

        if isinstance(self.left, SQLASTInterface):
            left_sql_ast = self.left.get_sql_ast()
        else:
            left_sql_ast = ['VALUE', self.left]  # pragma: no cover

        right_sql_ast = []
        if isinstance(self.right, SQLASTInterface):
            right_sql_ast = self.right.get_sql_ast()
        else:
            if self.operator in ('IN', 'NOT IN'):
                if isinstance(self.right, (tuple, list)):
                    right_sql_ast = ['VALUE', tuple(tuple(x) if isinstance(x, list) else x for x in self.right)]
            else:
                right_sql_ast = ['VALUE', self.right]

        return sql_ast + [left_sql_ast] + [right_sql_ast]

    def desc(self):
        return UnaryExpression(self, 'DESC')

    def asc(self):
        return UnaryExpression(self, 'ASC')

    def __neg__(self):
        op = get_neg_operator(self.operator, is_unary=False)
        if not op:
            return  # pragma: no cover
        return self.__class__(self.left, self.right, op)


BinaryExpression.BinaryExpression = BinaryExpression
