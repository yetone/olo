from olo.interfaces import SQLASTInterface
from olo.query import Query


def _order_by_to_str(item):
    if hasattr(item, 'attr_name'):
        return item.attr_name  # pragma: no cover

    _str = (
        item.value.attr_name if hasattr(item.value, 'attr_name')
        else _order_by_to_str(item.value)
    )

    if item.operator == 'DESC':
        return '-{}'.format(_str)

    return _str  # pragma: no cover


def order_by_to_strs(order_by):
    res = []
    for exp in order_by:
        res.append(_order_by_to_str(exp))
    return res


class CachedQuery(Query):

    def _can_be_cached(self):  # pylint: disable=too-many-return-statements
        if self._having_expressions:
            return False
        if self._on_expressions:
            return False
        if self._join:
            return False
        if (
                len(self._entities) != 1 or
                self._entities[0] is not self._model_class
        ):
            return False
        if self._group_by:
            return False  # pragma: no cover
        for exp in self._expressions:
            if not hasattr(exp, 'left'):
                return False  # pragma: no cover
            if not hasattr(exp.left, 'attr_name'):
                return False  # pragma: no cover
            if exp.operator not in (
                    '=', 'is'
            ):
                return False
            if hasattr(exp, 'right'):
                if isinstance(exp.right, SQLASTInterface):
                    return False
        return True

    def first(self):
        res = self.limit(1).all()
        return res[0] if res else None

    one = first

    def _get_expression_dict(self):
        return {
            exp.left.attr_name: exp.right
            for exp in self._expressions
        }

    def all(self):
        fallback = lambda: super(CachedQuery, self).all()  # noqa
        if not self._can_be_cached():
            return fallback()  # pragma: no cover
        order_by = order_by_to_strs(self._order_by)
        return self._model_class.cache.gets_by(
            order_by=order_by,
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
