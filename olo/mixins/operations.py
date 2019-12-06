from __future__ import annotations
from typing import TYPE_CHECKING, Type

if TYPE_CHECKING:
    from olo.expression import UnaryExpression, BinaryExpression


class UnaryOperationMixin(object):
    UnaryExpression: Type[UnaryExpression]

    def desc(self) -> UnaryExpression:
        return self.UnaryExpression(self, 'DESC')

    def asc(self) -> UnaryExpression:
        return self.UnaryExpression(self, 'ASC')


class BinaryOperationMixin(object):
    BinaryExpression: Type[BinaryExpression]

    def __add__(self, other):
        return self.BinaryExpression(self, other, '+')

    __radd__ = __add__

    def __sub__(self, other):
        return self.BinaryExpression(self, other, '-')

    __rsub__ = __sub__

    def __mul__(self, other):
        return self.BinaryExpression(self, other, '*')

    def __div__(self, other):
        return self.BinaryExpression(self, other, '/')

    __truediv__ = __div__

    def __mod__(self, other):
        return self.BinaryExpression(self, other, '%')

    def __eq__(self, other):
        operator = '='
        if other is None:
            operator = 'IS'
        return self.BinaryExpression(self, other, operator)

    def __ne__(self, other):
        operator = '!='
        if other is None:
            operator = 'IS NOT'
        return self.BinaryExpression(self, other, operator)

    def __gt__(self, other):
        return self.BinaryExpression(self, other, '>')

    def __ge__(self, other):
        return self.BinaryExpression(self, other, '>=')

    def __lt__(self, other):
        return self.BinaryExpression(self, other, '<')

    def __le__(self, other):
        return self.BinaryExpression(self, other, '<=')

    def in_(self, other):
        return self.BinaryExpression(self, other, 'IN')

    __lshift__ = in_

    def not_in_(self, other):
        return self.BinaryExpression(self, other, 'NOT IN')

    def like_(self, other):
        return self.BinaryExpression(self, other, 'LIKE')

    def ilike_(self, other):
        return self.BinaryExpression(self, other, 'ILIKE')

    def regexp_(self, other):
        return self.BinaryExpression(self, other, 'REGEXP')

    def between_(self, other):
        return self.BinaryExpression(self, other, 'BETWEEN')

    def concat_(self, other):
        return self.BinaryExpression(self, other, '||')

    def is_(self, other):
        return self.BinaryExpression(self, other, 'IS')

    def is_not_(self, other):
        return self.BinaryExpression(self, other, 'IS NOT')
