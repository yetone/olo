# pylint: disable=E0213,E1121

from __future__ import absolute_import, print_function, division

import operator
from functools import update_wrapper

from olo.compat import reduce
from olo.libs.compiler.eval import eval_src, get_prelude
from olo.libs.compiler.translators.ast_translator import ASTTranslator
from olo.libs.compiler.utils import throw

uops = [
    'not'
]

binops = [
    '+', '-', '*', '/', '>', '<', '==', '>=', '<=', '!=',
    '//', '**',
    'is', 'is not', 'in', 'not in'
]

binops_mapping = {
    'is': '==',
    'is not': '!=',
    'in': 'in_',
    'not in': 'not_in_',
}


def build_binop_factory(op):
    src = 'lambda x, y: x %s y'
    if op == 'in':
        src = 'lambda x, y: x.%s(y) if hasattr(x, "in_") else y.contains_(x)'
    elif op == 'not in':
        src = 'lambda x, y: x.%s(y) if hasattr(x, "not_in_") else y.not_contains_(x)'

    op = binops_mapping.get(op, op)

    return eval_src(src % op)


binop_factories = {
    op: build_binop_factory(op)
    for op in binops
}


def get_binop_factory(op):
    return binop_factories[op]


def priority(p):
    def decorator(func):
        def new_func(self, node):
            return func(self, node)
        return update_wrapper(new_func, func)
    return decorator


class TranslationError(Exception):
    pass


class QueryTranslator(ASTTranslator):

    def __init__(self, tree, scope):
        self.scope = scope
        self.locals = {}
        super(QueryTranslator, self).__init__(tree)
        self.dispatch(tree)

    def get_value(self, name, locals=None):
        locals = locals or self.locals
        if name in locals:
            return locals[name]
        return self.scope[name]

    def binop_factory(self, op, node):
        op = op.strip()
        factory = get_binop_factory(op)
        return lambda: factory(node.left.factory(), node.right.factory())

    def call(self, method, node):
        node.factory = method(node)

    def default_post(self, node):
        throw(NotImplementedError, node)

    def postGenExpr(self, node):
        return node.code.factory

    def postGenExprInner(self, node):
        def f():
            q = None
            for qual in node.quals:
                if q is None:
                    q = qual.factory()
                else:
                    q = q.flat_map(qual.factory())
            entities = node.expr.factory()
            if not isinstance(entities, (list, tuple)):
                entities = (entities,)
            q = q.map(*entities)
            return q
        return f

    def postGenExprFor(self, node):
        def f():
            model = node.iter.factory()
            if hasattr(model, 'model'):
                model = model.model
            self.locals[node.assign.name] = model

            q = model.cq

            if node.ifs:
                for if_ in node.ifs:
                    q = q.filter(if_.factory())

            return q
        return f

    def postGenExprIf(self, node):
        return node.test.factory

    def postIfExp(self, node):
        return '%s if %s else %s' % (
            node.then.src, node.test.src, node.else_.src)

    @priority(14)
    def postOr(self, node):
        return lambda: reduce(operator.or_, (
            x.factory() for x in node.nodes
        ))

    @priority(13)
    def postAnd(self, node):
        return lambda: reduce(operator.and_, (
            x.factory() for x in node.nodes
        ))

    @priority(12)
    def postNot(self, node):
        return lambda: not node.factory()

    @priority(11)
    def postCompare(self, node):
        def f():
            r = node.expr.factory()
            for op, expr in node.ops:
                r = get_binop_factory(op)(r, expr.factory())
            return r
        return f

    @priority(10)
    def postBitor(self, node):
        return lambda: reduce(operator.or_, (
            x.factory() for x in node.nodes
        ))

    @priority(9)
    def postBitxor(self, node):
        return lambda: reduce(operator.xor, (
            x.factory() for x in node.nodes
        ))

    @priority(8)
    def postBitand(self, node):
        return lambda: reduce(operator.and_, (
            x.factory() for x in node.nodes
        ))

    @priority(7)
    def postLeftShift(self, node):
        return self.binop_factory(' << ', node)

    @priority(7)
    def postRightShift(self, node):
        return self.binop_factory(' >> ', node)

    @priority(6)
    def postAdd(self, node):
        return self.binop_factory(' + ', node)

    @priority(6)
    def postSub(self, node):
        return self.binop_factory(' - ', node)

    @priority(5)
    def postMul(self, node):
        return self.binop_factory(' * ', node)

    @priority(5)
    def postDiv(self, node):
        return self.binop_factory(' / ', node)

    @priority(5)
    def postFloorDiv(self, node):
        return self.binop_factory(' // ', node)

    @priority(5)
    def postMod(self, node):
        return self.binop_factory(' % ', node)

    @priority(4)
    def postUnarySub(self, node):
        return lambda: -node.expr.factory()

    @priority(4)
    def postUnaryAdd(self, node):
        return lambda: +node.expr.factory()

    @priority(4)
    def postInvert(self, node):
        return lambda: ~node.expr.factory()

    @priority(3)
    def postPower(self, node):
        return self.binop_factory(' ** ', node)

    def postGetattr(self, node):
        node.priority = 2
        return lambda: getattr(node.expr.factory(), node.attrname)

    def postCallFunc(self, node):
        def f():
            node.priority = 2
            args = [arg.factory() for arg in node.args]

            kwargs = {}

            if node.star_args:
                args += node.star_args.factory()

            if node.dstar_args:
                kwargs = node.dstar_args.factory()

            return node.node.factory()(*args, **kwargs)

        return f

    def postSubscript(self, node):
        def f():
            assert node.flags == 'OP_APPLY'
            assert isinstance(node.subs, list)
            v = node.expr.factory()
            # TODO: support multi subs
            assert len(node.subs) == 1
            sub = node.subs[0]
            return v[sub.factory()]

        return f

    def postSlice(self, node):
        def f():
            node.priority = 2
            v = node.expr.factory()
            lower = node.lower.factory() if node.lower is not None else None
            upper = node.upper.factory() if node.upper is not None else None
            if lower is not None:
                if upper is not None:
                    return lambda: v[lower: upper]
                return v[lower:]
            if upper is not None:
                return lambda: v[:upper]

            return v[:]

        return f

    def postSliceobj(self, node):
        return lambda: slice(*(item.factory() for item in node.nodes))

    def postConst(self, node):
        node.priority = 1
        return lambda: node.value

    def postEllipsis(self, node):
        return Ellipsis

    def postList(self, node):
        node.priority = 1
        return lambda: [item.factory() for item in node.nodes]

    def postTuple(self, node):
        node.priority = 1
        return lambda: tuple([item.factory() for item in node.nodes])

    def postAssTuple(self, node):
        node.priority = 1
        return lambda: tuple([item.factory() for item in node.nodes])

    def postDict(self, node):
        node.priority = 1
        return lambda: {
            key.factory(): value.factory()
            for key, value in node.items
        }

    def postSet(self, node):
        node.priority = 1
        return lambda: {item.factory() for item in node.nodes}

    def postBackquote(self, node):
        node.priority = 1
        return node.expr.factory()

    def postName(self, node):
        node.priority = 1
        return lambda: self.get_value(node.name)

    def postAssName(self, node):
        node.priority = 1
        return lambda: self.get_value(node.name)


def ast2factory(tree, scope):
    scope.update(get_prelude())
    QueryTranslator(tree, scope)
    return tree.factory
