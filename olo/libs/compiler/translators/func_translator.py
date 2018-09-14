# pylint: disable=E0213

import types

from olo.compat import izip, iteritems
from olo.libs.decompiling import decompile
from olo.libs.compiler.translators.ast_translator import (
    priority, PythonTranslator
)
from olo.libs.compiler.eval import eval_src
from olo.libs.compiler import ast
from olo.libs.compiler.prelude_names import PL_TRANS_FUNC
from olo.utils import getargspec


CK_TRANS_RES = '_olo_func_trans'


def _bracket(src):
    return '(%s)' % src


class FuncTranslator(PythonTranslator):

    @priority(14)
    def postOr(translator, node):
        return ' | '.join(_bracket(expr.src) for expr in node.nodes)

    @priority(13)
    def postAnd(translator, node):
        return ' & '.join(_bracket(expr.src) for expr in node.nodes)

    @priority(12)
    def postNot(translator, node):
        return '!' + _bracket(node.expr.src)

    @priority(11)
    def postCompare(translator, node):
        is_method = False
        result = [node.expr.src]
        for op, expr in node.ops:
            if op not in ('in', 'not in'):
                result.extend((op, expr.src))
                continue

            is_method = True
            result[0] = _bracket(node.expr.src)
            if op == 'in':
                op = 'in_'
            elif op == 'not in':
                op = 'not_in_'
            elif op == 'is':
                op = '=='
            elif op == 'is not':
                op = '!='
            result.extend(('.', op, '(', expr.src, ')'))

        if is_method:
            return ''.join(result)

        return ' '.join(result)

    def postGenExpr(self, node):
        return node.code.src

    def postLambda(self, node):
        src = 'lambda %s: %s' % (','.join(node.argnames), node.code.src)
        return src

    def postCallFunc(translator, node):
        node.priority = 2
        args = [arg.src for arg in node.args]

        if node.star_args:
            args.append('*'+node.star_args.src)

        if node.dstar_args:
            args.append('**'+node.dstar_args.src)

        if len(args) == 1 and isinstance(node.args[0], ast.GenExpr):
            return node.node.src + args[0]

        return '%s(%s)(%s)' % (PL_TRANS_FUNC, node.node.src, ', '.join(args))


def transform_func(func):
    if not isinstance(func, types.FunctionType):
        return func

    res = getattr(func, CK_TRANS_RES, None)
    if res is not None:
        return res

    ast, _, _ = decompile(func)
    FuncTranslator(ast)

    argspec = getargspec(func)
    args = argspec.args
    defaults = argspec.defaults
    varargs = argspec.varargs
    keywords = argspec.varkw
    arg_str = ', '.join(filter(None, [
        ', '.join(args),
        ', '.join(
            '{}={}'.format(k, repr(v))
            for k, v in iteritems(defaults)
        ),
        '*%s' % varargs if varargs else '',
        '**%s' % keywords if keywords else '',
    ]))

    src = 'lambda {}: {}'.format(
        arg_str,
        ast.src
    )

    globals = func.__globals__
    if func.__closure__:
        globals = dict(globals, **dict(
            izip(
                func.func_code.co_freevars,
                (c.cell_contents for c in func.func_closure)
            )
        ))
    res = eval_src(src, globals=globals)
    setattr(func, CK_TRANS_RES, res)
    return res
