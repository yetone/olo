import math

from olo.interfaces import SQLLiteralInterface
from olo.mixins.operations import BinaryOperationMixin
from olo.utils import missing, sql_and_params
from olo.compat import with_metaclass
from olo.errors import SupportError


class FunctionMeta(type):
    def __call__(cls, *args, **kwargs):
        from olo.field import BaseField
        from olo.expression import Expression
        from olo.query import Query
        from olo.model import ModelMeta

        if (
                hasattr(cls, 'normal_func')
                and args
                and not isinstance(args[0], (
                    BaseField, Expression, Query, ModelMeta)
                )
        ):
            return cls.normal_func(*args, **kwargs)

        if len(args) != 1 or len(kwargs) > 0:
            raise SupportError(  # pragma: no cover
                'The function {}() only accepts a parameter: {}({})'.format(
                    cls.__name__, cls.__name__,
                    ', '.join(filter(None, [
                        ', '.join(map(repr, args)),
                        ', '.join(
                            '{}={}'.format(k, repr(v))
                            for k, v in kwargs.iteritems()
                        )]))
                )
            )

        arg = args[0]

        if isinstance(arg, ModelMeta) and cls.__name__ != 'COUNT':
            raise SupportError(
                'Function {}() cannot be applied to model: {}'.format(
                    cls.__name__, arg.__name__
                )
            )

        if isinstance(arg, Query):

            q = arg
            if cls.__name__ != 'COUNT' and (
                    len(q._entities) != 1 or not isinstance(
                        q._entities[0], SQLLiteralInterface
                    )
            ):
                raise SupportError(
                    'Function {}() cannot be applied'
                    ' to query: {}.query({})'.format(
                        cls.__name__,
                        q._model_class.__name__,
                        ', '.join(map(repr, q._entities))
                    )
                )
            try:
                q = q.map(cls.__call__(*q._entities))
            except SupportError:  # pragma: no cover
                raise SupportError(  # pragma: no cover
                    'Function {}() cannot be applied'
                    ' to query: {}.query'.format(
                        cls.__name__, q._model_class.__name__
                    )
                )
            return q
        return super(FunctionMeta, cls).__call__(*args, **kwargs)


class Function(
        with_metaclass(
            FunctionMeta, SQLLiteralInterface, BinaryOperationMixin
        )
):

    from olo.expression import BinaryExpression  # noqa

    def __init__(self, *args):
        self.args = args

    def alias(self, name):
        inst = self.__class__(*self.args)
        inst._alias_name = name
        return inst

    def _get_sql_and_params(self):
        pieces = []
        params = []

        for arg in self.args:
            piece, _params = sql_and_params(arg, coerce=repr)

            pieces.append(piece)
            if _params:
                params.extend(_params)

        s = '{}({})'.format(
            self.__class__.__name__.upper(),
            ', '.join(pieces)
        )

        return s, params

    def get_sql_and_params(self):
        s, params = self._get_sql_and_params()

        if self.alias_name:
            return '{} AS {}'.format(
                s, self.alias_name
            ), params
        return s, params


class COUNT(Function):
    def _get_sql_and_params(self):
        from olo.model import ModelMeta
        from olo.expression import BinaryExpression

        if len(self.args) == 1:
            if isinstance(
                    self.args[0], ModelMeta
            ):
                return 'COUNT(1)', []
            if isinstance(
                    self.args[0], BinaryExpression
            ):
                ifexp = IF(self.args[0]).THEN(1).ELSE(None)
                _str, params = ifexp.get_sql_and_params()
                return 'COUNT({})'.format(_str), params
        return super(COUNT, self)._get_sql_and_params()


class SUM(Function):
    pass


class DISTINCT(Function):
    pass


class AVG(Function):
    pass


class SQRT(Function):
    normal_func = math.sqrt


class MAX(Function):
    normal_func = max


class MIN(Function):
    normal_func = min


class LENGTH(Function):
    normal_func = len


class RAND(Function):
    pass


def _get_sql_and_params(item):
    if isinstance(item, SQLLiteralInterface):
        return item.get_sql_and_params()  # pragma: no cover
    return '%s', [item]


class IF(Function):
    def __init__(self, *args):
        assert len(args) == 1

        super(IF, self).__init__(*args)

        test = args[0]
        self._test = test
        self._then = missing
        self._else = missing

    def alias(self, name):
        inst = self.__class__(*self.args)
        inst._alias_name = name
        inst._then = self._then
        inst._else = self._else
        return inst

    def THEN(self, _then):
        self._then = _then
        return self

    def ELSE(self, _else):
        self._else = _else
        return self

    def get_sql_and_params(self):
        params = []

        test_str, test_params = sql_and_params(self._test)

        s = 'CASE WHEN {}'.format(test_str)
        params += test_params

        if self._then is not missing:
            _str, _params = _get_sql_and_params(self._then)
            s += ' THEN {}'.format(_str)
            params += _params

        if self._else is not missing:
            _str, _params = _get_sql_and_params(self._else)
            s += ' ELSE {}'.format(_str)
            params += _params

        s += ' END'

        if self.alias_name:
            return '({}) AS {}'.format(
                s, self.alias_name
            ), params

        return s, params


g = globals()


# pylint: disable=E0602
def attach_func_method(cls):
    for name in [
            'max', 'min', 'avg', 'count', 'sum',
            'length', 'distinct', 'sqrt',
    ]:
        setattr(cls, name, (
            lambda name:
            lambda self, *args, **kwargs:
            g[name.upper()](self, *args, **kwargs)
        )(name))
    return cls


attach_func_method(Function)
