# pylint: disable=E0213,E1121,C0301

# Copyright (c) 2016 Pony ORM, LLC. All rights reserved. team (at) ponyorm.com
#
# Most of the content of this file is copied from https://github.com/ponyorm/pony/blob/orm/pony/orm/asttranslation.py
# and is released under the Apache 2.0 license: https://www.apache.org/licenses/LICENSE-2.0

from __future__ import absolute_import, print_function, division

from functools import update_wrapper

from olo.libs.compiler import ast

from olo.libs.compiler.utils import throw


class TranslationError(Exception):
    pass


class ASTTranslator(object):
    def __init__(self, tree):
        self.tree = tree
        self.pre_methods = {}
        self.post_methods = {}

    def dispatch(self, node):
        cls = node.__class__

        try:
            pre_method = self.pre_methods[cls]
        except KeyError:
            pre_method = getattr(self, 'pre' + cls.__name__, self.default_pre)
            self.pre_methods[cls] = pre_method

        stop = self.call(pre_method, node)

        if stop:
            return

        for child in node.getChildNodes():
            self.dispatch(child)

        try:
            post_method = self.post_methods[cls]
        except KeyError:
            post_method = getattr(
                self, 'post' + cls.__name__, self.default_post)
            self.post_methods[cls] = post_method
        self.call(post_method, node)

    def call(self, method, node):
        return method(node)

    def default_pre(self, node):
        pass

    def default_post(self, node):
        pass


def priority(p):
    def decorator(func):
        def new_func(translator, node):
            node.priority = p
            for child in node.getChildNodes():
                if getattr(child, 'priority', 0) >= p:
                    child.src = '(%s)' % child.src
            return func(translator, node)
        return update_wrapper(new_func, func)
    return decorator


def binop_src(op, node):
    return op.join((node.left.src, node.right.src))


def ast2src(tree):
    PythonTranslator(tree)
    return tree.src


class PythonTranslator(ASTTranslator):
    def __init__(translator, tree):
        super(PythonTranslator, translator).__init__(tree)
        translator.dispatch(tree)

    def call(translator, method, node):
        node.src = method(node)

    def default_post(translator, node):
        throw(NotImplementedError, node)

    def postGenExpr(translator, node):
        return '(%s)' % node.code.src

    def postGenExprInner(translator, node):
        return node.expr.src + ' ' + ' '.join(qual.src for qual in node.quals)

    def postGenExprFor(translator, node):
        src = 'for %s in %s' % (node.assign.src, node.iter.src)
        if node.ifs:
            ifs = ' '.join(if_.src for if_ in node.ifs)
            src += ' ' + ifs
        return src

    def postGenExprIf(translator, node):
        return 'if %s' % node.test.src

    def postIfExp(translator, node):
        return '%s if %s else %s' % (
            node.then.src, node.test.src, node.else_.src)

    @priority(14)
    def postOr(translator, node):
        return ' or '.join(expr.src for expr in node.nodes)

    @priority(13)
    def postAnd(translator, node):
        return ' and '.join(expr.src for expr in node.nodes)

    @priority(12)
    def postNot(translator, node):
        return 'not ' + node.expr.src

    @priority(11)
    def postCompare(translator, node):
        result = [node.expr.src]
        for op, expr in node.ops:
            result.extend((op, expr.src))
        return ' '.join(result)

    @priority(10)
    def postBitor(translator, node):
        return ' | '.join(expr.src for expr in node.nodes)

    @priority(9)
    def postBitxor(translator, node):
        return ' ^ '.join(expr.src for expr in node.nodes)

    @priority(8)
    def postBitand(translator, node):
        return ' & '.join(expr.src for expr in node.nodes)

    @priority(7)
    def postLeftShift(translator, node):
        return binop_src(' << ', node)

    @priority(7)
    def postRightShift(translator, node):
        return binop_src(' >> ', node)

    @priority(6)
    def postAdd(translator, node):
        return binop_src(' + ', node)

    @priority(6)
    def postSub(translator, node):
        return binop_src(' - ', node)

    @priority(5)
    def postMul(translator, node):
        return binop_src(' * ', node)

    @priority(5)
    def postDiv(translator, node):
        return binop_src(' / ', node)

    @priority(5)
    def postFloorDiv(translator, node):
        return binop_src(' // ', node)

    @priority(5)
    def postMod(translator, node):
        return binop_src(' % ', node)

    @priority(4)
    def postUnarySub(translator, node):
        return '-' + node.expr.src

    @priority(4)
    def postUnaryAdd(translator, node):
        return '+' + node.expr.src

    @priority(4)
    def postInvert(translator, node):
        return '~' + node.expr.src

    @priority(3)
    def postPower(translator, node):
        return binop_src(' ** ', node)

    def postGetattr(translator, node):
        node.priority = 2
        return '.'.join((node.expr.src, node.attrname))

    def postCallFunc(translator, node):
        node.priority = 2
        args = [arg.src for arg in node.args]

        if node.star_args:
            args.append('*'+node.star_args.src)

        if node.dstar_args:
            args.append('**'+node.dstar_args.src)

        # if len(args) == 1 and isinstance(node.args[0], ast.GenExpr):
        #     return node.node.src + args[0]

        return '%s(%s)' % (node.node.src, ', '.join(args))

    def postSubscript(translator, node):
        node.priority = 2
        if len(node.subs) == 1:
            sub = node.subs[0]
            if (
                    isinstance(sub, ast.Const) and
                    type(sub.value) is tuple and len(sub.value) > 1
            ):
                key = sub.src
                assert key.startswith('(') and key.endswith(')')
                key = key[1:-1]
            else:
                key = sub.src
        else:
            key = ', '.join([sub.src for sub in node.subs])
        return '%s[%s]' % (node.expr.src, key)

    def postSlice(translator, node):
        node.priority = 2
        lower = node.lower.src if node.lower is not None else ''
        upper = node.upper.src if node.upper is not None else ''
        return '%s[%s:%s]' % (node.expr.src, lower, upper)

    def postSliceobj(translator, node):
        return ':'.join(item.src for item in node.nodes)

    def postConst(translator, node):
        node.priority = 1
        value = node.value
        if type(value) is float:  # for Python < 2.7
            s = str(value)
            if float(s) == value:
                return s
        return repr(value)

    def postEllipsis(translator, node):
        return '...'

    def postList(translator, node):
        node.priority = 1
        return '[%s]' % ', '.join(item.src for item in node.nodes)

    def postTuple(translator, node):
        node.priority = 1
        if len(node.nodes) == 1:
            return '(%s,)' % node.nodes[0].src
        else:
            return '(%s)' % ', '.join(item.src for item in node.nodes)

    def postAssTuple(translator, node):
        node.priority = 1
        if len(node.nodes) == 1:
            return '(%s,)' % node.nodes[0].src
        else:
            return '(%s)' % ', '.join(item.src for item in node.nodes)

    def postDict(translator, node):
        node.priority = 1
        return '{%s}' % ', '.join(
            '%s: %s' % (key.src, value.src) for key, value in node.items
        )

    def postSet(translator, node):
        node.priority = 1
        return '{%s}' % ', '.join(item.src for item in node.nodes)

    def postBackquote(translator, node):
        node.priority = 1
        return '`%s`' % node.expr.src

    def postName(translator, node):
        node.priority = 1
        return node.name

    def postAssName(translator, node):
        node.priority = 1
        return node.name

    def postKeyword(translator, node):
        return '='.join((node.name, node.expr.src))
