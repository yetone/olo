import math

from six import with_metaclass

from olo.interfaces import SQLASTInterface
from olo.mixins.operations import BinaryOperationMixin
from olo.utils import missing
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
                        q._entities[0], SQLASTInterface
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
            FunctionMeta, BinaryOperationMixin,
            SQLASTInterface
        )
):

    from olo.expression import BinaryExpression  # noqa

    def __init__(self, *args):
        self.args = args

    def alias(self, name):
        inst = self.__class__(*self.args)
        inst._alias_name = name
        return inst

    def _get_sql_ast(self):
        sql_ast = [
            'CALL',
            self.__class__.__name__.upper()
        ] + [arg.get_sql_ast() if isinstance(arg, SQLASTInterface) else arg
             for arg in self.args]
        return sql_ast

    def get_sql_ast(self):
        sql_ast = self._get_sql_ast()
        if self.alias_name:
            sql_ast = ['ALIAS', sql_ast, self.alias_name]
        return sql_ast


class COUNT(Function):
    def _get_sql_ast(self):
        from olo.model import ModelMeta
        from olo.expression import BinaryExpression

        if len(self.args) == 1:
            if isinstance(
                    self.args[0], ModelMeta
            ):
                return ['CALL', 'COUNT', ['VALUE', 1]]
            if isinstance(
                    self.args[0], BinaryExpression
            ):
                ifexp = IF(self.args[0]).THEN(1).ELSE(None)
                sql_ast = ifexp.get_sql_ast()
                return ['CALL', 'COUNT', sql_ast]
            if isinstance(
                self.args[0], SQLASTInterface
            ):
                return ['CALL', 'COUNT', self.args[0].get_sql_ast()]
            return ['CALL', 'COUNT', ['VALUE', self.args[0]]]
        return super(COUNT, self)._get_sql_ast()


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

    def _get_sql_ast(self):
        if hasattr(self._test, 'get_sql_ast'):
            test_ast = self._test.get_sql_ast()
        else:
            test_ast = ['VALUE', self._test]  # pragma: no cover

        if hasattr(self._then, 'get_sql_ast'):
            then_ast = self._then.get_sql_ast()  # pragma: no cover
        else:
            then_ast = ['VALUE', self._then]

        if hasattr(self._else, 'get_sql_ast'):
            else_ast = self._else.get_sql_ast()  # pragma: no cover
        else:
            else_ast = ['VALUE', self._else]

        return [
            'IF',
            test_ast,
            then_ast,
            else_ast
        ]


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
