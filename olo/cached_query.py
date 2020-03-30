from olo.errors import ORMError
from olo.expression import BinaryExpression
from olo.interfaces import SQLASTInterface
from olo.query import Query
from olo.field import Field


class CachedQuery(Query):

    def _can_be_cached(self):  # pylint: disable=too-many-return-statements
        if self._having_expression is not None:
            return False
        if self._join_chain:
            return False
        if (
                len(self._entities) != 1 or
                self._entities[0] is not self._model_class
        ):
            return False
        if self._group_by:
            return False  # pragma: no cover
        exps = [self._expression]
        while exps:
            exp = exps.pop()
            if exp is None:
                continue

            if not isinstance(exp, BinaryExpression):
                return False  # pragma: no cover

            if isinstance(exp.left, BinaryExpression):
                if exp.operator != 'AND':
                    return False
                if not isinstance(exp.right, BinaryExpression):
                    return False
                exps.append(exp.left)
                exps.append(exp.right)
                continue

            if isinstance(exp.left, Field):
                if exp.operator not in (
                        '=', 'is'
                ):
                    return False
                if isinstance(exp.right, SQLASTInterface):
                    return False
                continue

            return False
        return True

    def first(self):
        res = self.limit(1).all()
        return res[0] if res else None

    one = first

    def _get_expression_dict(self):
        res = {}
        exps = [self._expression]
        while exps:
            exp = exps.pop()
            if exp is None:
                continue

            if isinstance(exp.left, BinaryExpression):
                exps.append(exp.left)
                exps.append(exp.right)
                continue

            if isinstance(exp.left, Field):
                res[exp.left.attr_name] = exp.right
                continue

            raise ORMError('cannot access here!!!')
        return res

    def all(self):
        fallback = lambda: super(CachedQuery, self).all()  # noqa
        if not self._can_be_cached():
            return fallback()  # pragma: no cover
        return self._model_class.cache.gets_by(
            order_by=self._order_by,
            start=self._offset,
            limit=self._limit,
            **self._get_expression_dict()
        )

    def count(self):
        if not self._can_be_cached():
            return super(CachedQuery, self).count()  # pragma: no cover
        return self._model_class.cache.count_by(
            **self._get_expression_dict()
        )

    __len__ = count
