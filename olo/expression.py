from functools import wraps
from olo.interfaces import SQLLiteralInterface
from olo.mixins.operations import BinaryOperationMixin
from olo.utils import (
    compare_operator_precedence, sql_and_params, get_neg_operator
)


__all__ = ['UnaryExpression', 'BinaryExpression']


def _auto_transform_to_bin_exp(func):
    @wraps(func)
    def wrapper(self, other):
        other = transform_to_bin_exp(other)
        return func(self, other)
    return wrapper


class Expression(SQLLiteralInterface):
    def __neg__(self):
        return  # pragma: no cover


class UnaryExpression(Expression):

    def __init__(self, value, operator):
        self.value = value
        self.operator = operator

    def get_sql_and_params(self):
        left_str, params = sql_and_params(self.value)
        alias_name = getattr(self.value, 'alias_name', None)
        if alias_name:
            left_str = alias_name
        return '{} {}'.format(left_str, self.operator), params

    def __neg__(self):
        op = get_neg_operator(self.operator, is_unary=True)  # pragma: no cover
        if not op:  # pragma: no cover
            return  # pragma: no cover
        return self.__class__(self.value, op)  # pragma: no cover


def transform_to_bin_exp(item):
    from olo.field import ConstField
    from olo import funcs

    if isinstance(item, BinaryExpression):
        return item

    if isinstance(item, bool):
        return ConstField(int(item))

    field = ConstField(item)

    if isinstance(item, (int, long, float)):
        return field != 0

    if isinstance(item, basestring):
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

    def get_sql_and_params(self):
        from .query import Query

        if isinstance(self.left, Expression) and self.operator == '=':
            """Optimize:
            `(a IN (1, 2)) = True` => `a IN (1, 2)`
            `(a IN (1, 2)) = False` => `a NOT IN (1, 2)`
            """
            if self.right is True:
                return self.left.get_sql_and_params()
            elif self.right is False:
                nleft = -self.left
                if nleft:
                    return nleft.get_sql_and_params()

        params = []

        if isinstance(self.left, SQLLiteralInterface):
            left_str, _params = self.left.get_sql_and_params()

            if _params:
                params.extend(_params)

            if self.left.alias_name:
                left_str = self.left.alias_name
            elif isinstance(self.left, Expression):
                _cmp = compare_operator_precedence(
                    self.left.operator, self.operator
                )
                if _cmp < 0:
                    left_str = '({})'.format(left_str)
        else:
            left_str = '%s'  # pragma: no cover

        if isinstance(self.right, SQLLiteralInterface):
            right_str, _params = self.right.get_sql_and_params()

            if _params:
                params.extend(_params)

            if self.right.alias_name:
                right_str = self.right.alias_name
            elif isinstance(self.right, Expression):
                _cmp = compare_operator_precedence(
                    self.right.operator, self.operator
                )
                if _cmp < 0:
                    right_str = '({})'.format(right_str)
            elif isinstance(self.right, Query):
                right_str = '({})'.format(right_str)
        else:
            right_str = '%s'
            params.append(self.right)

        return '{} {} {}'.format(
            left_str,
            self.operator,
            right_str,
        ), params

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
