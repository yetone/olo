# pylint: disable=E0213,E1121,E1101,C0301

# Copyright (c) 2016 Pony ORM, LLC. All rights reserved. team (at) ponyorm.com
#
# Most of the content of this file is copied from https://github.com/ponyorm/pony/blob/orm/pony/orm/decompiling.py
# and is released under the Apache 2.0 license: https://www.apache.org/licenses/LICENSE-2.0

from __future__ import absolute_import, print_function, division
from olo.compat import PY2, PY310, PY311, izip, xrange

import sys
import types
from opcode import opname as opnames, HAVE_ARGUMENT, EXTENDED_ARG, cmp_op
from opcode import hasconst, hasname, hasjrel, haslocal, hascompare, hasfree, hasjabs

from olo.libs.cache import LocalCache
from olo.libs.compiler import ast
from olo.libs.compiler.utils import throw


def get_codeobject_id(obj):
    return id(obj)


ast_cache = LocalCache()


def decompile(x):
    cells = {}
    if isinstance(x, types.CodeType):
        codeobject = x
    elif isinstance(x, types.GeneratorType):
        codeobject = x.gi_frame.f_code
    elif isinstance(x, types.FunctionType):
        codeobject = x.func_code if PY2 else x.__code__
        if PY2:
            if x.func_closure:
                cells = dict(izip(codeobject.co_freevars, x.func_closure))
        else:
            if x.__closure__:
                cells = dict(izip(codeobject.co_freevars, x.__closure__))
    else:
        throw(TypeError)
    key = get_codeobject_id(codeobject)
    result = ast_cache.get(key)
    if result is None:
        decompiler = Decompiler(codeobject)
        result = decompiler.ast, decompiler.external_names
        ast_cache.set(key, result)
    return result + (cells,)


def simplify(clause):
    if isinstance(clause, ast.And):
        if len(clause.nodes) == 1:
            result = clause.nodes[0]
        else:
            return clause
    elif isinstance(clause, ast.Or):
        if len(clause.nodes) == 1:
            result = ast.Not(clause.nodes[0])
        else:
            return clause
    else:
        return clause
    if getattr(result, 'endpos', 0) < clause.endpos:
        result.endpos = clause.endpos
    return result


class InvalidQuery(Exception):
    pass


class AstGenerated(Exception):
    pass


def binop(node_type, args_holder=tuple):
    def method(decompiler):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        return node_type(args_holder((oper1, oper2)))
    return method


if not PY2:
    ord = lambda x: x  # noqa


class Decompiler(object):
    def __init__(decompiler, code, start=0, end=None):
        decompiler.code = code
        decompiler.start = decompiler.pos = start
        if end is None:
            end = len(code.co_code)
        decompiler.end = end
        decompiler.stack = []
        decompiler.targets = {}
        decompiler.ast = None
        decompiler.names = set()
        decompiler.assnames = set()
        decompiler.decompile()
        decompiler.ast = decompiler.stack.pop()
        decompiler.external_names = decompiler.names - decompiler.assnames
        assert not decompiler.stack, decompiler.stack

    def decompile(decompiler):
        PY36 = sys.version_info >= (3, 6)
        code = decompiler.code
        co_code = code.co_code
        free = code.co_cellvars + code.co_freevars
        try:
            extended_arg = 0
            while decompiler.pos < decompiler.end:
                i = decompiler.pos
                if i in decompiler.targets:
                    decompiler.process_target(i)
                op = ord(code.co_code[i])
                if PY36:
                    if op >= HAVE_ARGUMENT:
                        oparg = ord(co_code[i + 1]) | extended_arg
                        extended_arg = (
                            (extended_arg << 8) if op == EXTENDED_ARG else 0
                        )
                    i += 2
                else:
                    i += 1
                    if op >= HAVE_ARGUMENT:
                        oparg = ord(co_code[i]) + ord(co_code[i + 1]) * 256
                        i += 2
                        if op == EXTENDED_ARG:
                            op = ord(code.co_code[i])
                            i += 1
                            oparg = (
                                ord(co_code[i]) + ord(co_code[i + 1]) * 256 +
                                oparg * 65536
                            )
                            i += 2

                opname = opnames[op].replace('+', '_')
                if op >= HAVE_ARGUMENT:
                    if op in hasconst:
                        arg = [code.co_consts[oparg]]
                    elif op in hasname:
                        if opname == 'LOAD_GLOBAL':
                            push_null = False
                            if PY311:
                                push_null = oparg & 1
                                oparg >>= 1
                            arg = [code.co_names[oparg], push_null]
                        else:
                            arg = [code.co_names[oparg]]
                    elif op in hasjrel:
                        arg = [i + oparg * (2 if PY310 else 1)
                               * (-1 if 'BACKWARD' in opname else 1)]
                    elif op in haslocal:
                        arg = [code.co_varnames[oparg]]
                    elif op in hascompare:
                        arg = [cmp_op[oparg]]
                    elif op in hasfree:
                        if PY311:
                            oparg -= len(code.co_varnames)
                        arg = [free[oparg]]
                    elif op in hasjabs:
                        arg = [oparg * (2 if PY310 else 1)]
                    else:
                        arg = [oparg]
                else:
                    arg = []

                opname = opnames[op].replace('+', '_')
                # print(opname, arg, decompiler.stack)
                method = getattr(decompiler, opname, None)
                if method is None:
                    throw(NotImplementedError, 'Unsupported operation: %s' % opname)  # noqa
                decompiler.pos = i
                x = method(*arg)
                if x is not None:
                    decompiler.stack.append(x)
        except AstGenerated:
            pass

    def pop_items(decompiler, size):
        if not size:
            return ()
        result = decompiler.stack[-size:]
        decompiler.stack[-size:] = []
        return result

    def store(decompiler, node):
        stack = decompiler.stack
        if not stack:
            stack.append(node)
            return
        top = stack[-1]
        if (
                isinstance(top, (ast.AssTuple, ast.AssList)) and
                len(top.nodes) < top.count
        ):
            top.nodes.append(node)
            if len(top.nodes) == top.count:
                decompiler.store(stack.pop())
        elif isinstance(top, ast.GenExprFor):
            assert top.assign is None
            top.assign = node
        else:
            stack.append(node)

    BINARY_POWER        = binop(ast.Power)  # noqa
    BINARY_MULTIPLY     = binop(ast.Mul)  # noqa
    BINARY_DIVIDE       = binop(ast.Div)  # noqa
    BINARY_FLOOR_DIVIDE = binop(ast.FloorDiv)  # noqa
    BINARY_ADD          = binop(ast.Add)  # noqa
    BINARY_SUBTRACT     = binop(ast.Sub)  # noqa
    BINARY_LSHIFT       = binop(ast.LeftShift)  # noqa
    BINARY_RSHIFT       = binop(ast.RightShift)  # noqa
    BINARY_AND          = binop(ast.Bitand)  # noqa
    BINARY_XOR          = binop(ast.Bitxor)  # noqa
    BINARY_OR           = binop(ast.Bitor)  # noqa
    BINARY_TRUE_DIVIDE  = BINARY_DIVIDE  # noqa
    BINARY_MODULO       = binop(ast.Mod)  # noqa

    def BINARY_SUBSCR(decompiler):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        if isinstance(oper2, ast.Tuple):
            return ast.Subscript(oper1, 'OP_APPLY', list(oper2.nodes))
        return ast.Subscript(oper1, 'OP_APPLY', [oper2])

    def BUILD_CONST_KEY_MAP(decompiler, length):
        keys = decompiler.stack.pop()
        assert isinstance(keys, ast.Const)
        keys = [ast.Const(key) for key in keys.value]
        values = decompiler.pop_items(length)
        pairs = list(izip(keys, values))
        return ast.Dict(pairs)

    def BUILD_LIST(decompiler, size):
        return ast.List(decompiler.pop_items(size))

    def BUILD_MAP(decompiler, length):
        if sys.version_info < (3, 5):
            return ast.Dict(())
        data = decompiler.pop_items(2 * length)  # noqa [key1, value1, key2, value2, ...]
        it = iter(data)
        pairs = list(izip(it, it))  # [(key1, value1), (key2, value2), ...]
        return ast.Dict(pairs)

    def BUILD_SET(decompiler, size):
        return ast.Set(decompiler.pop_items(size))

    def BUILD_SLICE(decompiler, size):
        return ast.Sliceobj(decompiler.pop_items(size))

    def BUILD_TUPLE(decompiler, size):
        return ast.Tuple(decompiler.pop_items(size))

    def CALL_FUNCTION(decompiler, argc, star=None, star2=None):
        pop = decompiler.stack.pop
        kwarg, posarg = divmod(argc, 256)
        args = []
        for i in xrange(kwarg):
            arg = pop()
            key = pop().value
            args.append(ast.Keyword(key, arg))
        for i in xrange(posarg):
            args.append(pop())
        args.reverse()
        return decompiler._call_function(args, star, star2)

    def _call_function(decompiler, args, star=None, star2=None):
        tos = decompiler.stack.pop()
        if isinstance(tos, ast.GenExpr):
            assert len(args) == 1 and star is None and star2 is None
            genexpr = tos
            qual = genexpr.code.quals[0]
            assert isinstance(qual.iter, ast.Name)
            assert qual.iter.name in ('.0', '[outmost-iterable]')
            qual.iter = args[0]
            return genexpr
        return ast.CallFunc(tos, args, star, star2)

    def CALL_FUNCTION_VAR(decompiler, argc):
        return decompiler.CALL_FUNCTION(argc, decompiler.stack.pop())

    def CALL_FUNCTION_KW(decompiler, argc):
        if sys.version_info < (3, 6):
            return decompiler.CALL_FUNCTION(argc, star2=decompiler.stack.pop())
        keys = decompiler.stack.pop()
        assert isinstance(keys, ast.Const)
        keys = keys.value
        values = decompiler.pop_items(argc)
        assert len(keys) <= len(values)
        args = values[:-len(keys)]
        for key, value in izip(keys, values[-len(keys):]):
            args.append(ast.Keyword(key, value))
        return decompiler._call_function(args)

    def CALL_FUNCTION_VAR_KW(decompiler, argc):
        star2 = decompiler.stack.pop()
        star = decompiler.stack.pop()
        return decompiler.CALL_FUNCTION(argc, star, star2)

    def CALL_FUNCTION_EX(decompiler, argc):
        star2 = None
        if argc:
            if argc != 1:
                throw(NotImplementedError)
            star2 = decompiler.stack.pop()
        star = decompiler.stack.pop()
        return decompiler._call_function([], star, star2)

    def CALL_METHOD(decompiler, argc):
        pop = decompiler.stack.pop
        args = []
        if argc >= 256:
            kwargc = argc // 256
            argc = argc % 256
            for i in range(kwargc):
                v = pop()
                k = pop()
                assert isinstance(k, ast.Const)
                k = k.value # ast.Name(k.value)
                args.append(ast.Keyword(k, v))
        for i in range(argc):
            args.append(pop())
        args.reverse()
        method = pop()
        return ast.CallFunc(method, args)

    def CACHE(decompiler):
        pass

    def CALL(decompiler, argc):
        values = decompiler.pop_items(argc)

        keys = decompiler.kw_names
        decompiler.kw_names = None

        args = values
        keywords = []
        if keys:
            args = values[:-len(keys)]
            keywords = [ast.keyword(k, v) for k, v in zip(keys, values[-len(keys):])]

        self = decompiler.stack.pop()
        callable_ = decompiler.stack.pop()
        if callable_ is None:
            callable_ = self
        else:
            args.insert(0, self)
        decompiler.stack.append(callable_)
        return decompiler._call_function(args, keywords)

    def COMPARE_OP(decompiler, op):
        oper2 = decompiler.stack.pop()
        oper1 = decompiler.stack.pop()
        return ast.Compare(oper1, [(op, oper2)])

    def COPY_FREE_VARS(decompiler, n):
        pass

    def DUP_TOP(decompiler):
        return decompiler.stack[-1]

    def FOR_ITER(decompiler, endpos):
        assign = None
        iter = decompiler.stack.pop()
        ifs = []
        return ast.GenExprFor(assign, iter, ifs)

    def GET_ITER(decompiler):
        pass

    def JUMP_IF_FALSE(decompiler, endpos):
        return decompiler.conditional_jump(endpos, ast.And)

    JUMP_IF_FALSE_OR_POP = JUMP_IF_FALSE

    def JUMP_IF_TRUE(decompiler, endpos):
        return decompiler.conditional_jump(endpos, ast.Or)

    JUMP_IF_TRUE_OR_POP = JUMP_IF_TRUE

    def conditional_jump(decompiler, endpos, clausetype):
        i = decompiler.pos  # next instruction
        if i in decompiler.targets:
            decompiler.process_target(i)
        expr = decompiler.stack.pop()
        clause = clausetype([expr])
        clause.endpos = endpos
        decompiler.targets.setdefault(endpos, clause)
        return clause

    def process_target(decompiler, pos, partial=False):
        if pos is None:
            limit = None
        elif partial:
            limit = decompiler.targets.get(pos, None)
        else:
            limit = decompiler.targets.pop(pos, None)
        top = decompiler.stack.pop()
        while True:
            top = simplify(top)
            if top is limit:
                break
            if isinstance(top, ast.GenExprFor):
                break

            top2 = decompiler.stack[-1]
            if isinstance(top2, ast.GenExprFor):
                break
            if partial and hasattr(top2, 'endpos') and top2.endpos == pos:
                break

            if isinstance(top2, (ast.And, ast.Or)):
                if top2.__class__ == top.__class__:
                    top2.nodes.extend(top.nodes)
                else:
                    top2.nodes.append(top)
            elif isinstance(top2, ast.IfExp):  # Python 2.5
                top2.else_ = top
                if hasattr(top, 'endpos'):
                    top2.endpos = top.endpos
                    if decompiler.targets.get(top.endpos) is top:
                        decompiler.targets[top.endpos] = top2
            else:
                throw(NotImplementedError, 'Expression is too complex to decompile, try to pass query as string, e.g. select("x for x in Something")')  # noqa
            top2.endpos = max(top2.endpos, getattr(top, 'endpos', 0))
            top = decompiler.stack.pop()
        decompiler.stack.append(top)

    def JUMP_FORWARD(decompiler, endpos):
        i = decompiler.pos  # next instruction
        decompiler.process_target(i, True)
        then = decompiler.stack.pop()
        decompiler.process_target(i, False)
        test = decompiler.stack.pop()
        if_exp = ast.IfExp(simplify(test), simplify(then), None)
        if_exp.endpos = endpos
        decompiler.targets.setdefault(endpos, if_exp)
        if decompiler.targets.get(endpos) is then:
            decompiler.targets[endpos] = if_exp
        return if_exp


    def LIST_APPEND(decompiler, offset):
        tos = decompiler.stack.pop()
        list_node = decompiler.stack[-offset]
        if isinstance(list_node, ast.comprehension):
            throw(InvalidQuery('Use generator expression (... for ... in ...) '
                               'instead of list comprehension [... for ... in ...] inside query'))

        assert isinstance(list_node, ast.List), list_node
        list_node.elts.append(tos)

    def LIST_EXTEND(decompiler, offset):
        if offset != 1:
            raise NotImplementedError(offset)
        items = decompiler.stack.pop()
        if not isinstance(items, ast.Constant):
            raise NotImplementedError(type(items))
        if not isinstance(items.value, tuple):
            raise NotImplementedError(type(items.value))
        lst = decompiler.stack.pop()
        if not isinstance(lst, ast.List):
            raise NotImplementedError(type(lst))
        values = [make_const(v) for v in items.value]
        lst.elts.extend(values)
        return lst

    def LIST_TO_TUPLE(decompiler):
        tos = decompiler.stack.pop()
        if not isinstance(tos, ast.List):
            throw(InvalidQuery, "Translation error, please contact developers: list expected, got: %r" % tos)
        return ast.Tuple(tos.elts, ast.Load())

    def LOAD_ATTR(decompiler, attr_name):
        return ast.Getattr(decompiler.stack.pop(), attr_name)

    def LOAD_CLOSURE(decompiler, freevar):
        decompiler.names.add(freevar)
        return ast.Name(freevar)

    def LOAD_CONST(decompiler, const_value):
        return ast.Const(const_value)

    def LOAD_DEREF(decompiler, freevar):
        decompiler.names.add(freevar)
        return ast.Name(freevar)

    def LOAD_FAST(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname)

    def LOAD_GLOBAL(decompiler, varname, push_null):
        if push_null:
            decompiler.stack.append(None)
        decompiler.names.add(varname)
        return ast.Name(varname, ast.Load())

    def LOAD_METHOD(decompiler, methname):
        return decompiler.LOAD_ATTR(methname)

    def LOAD_NAME(decompiler, varname):
        decompiler.names.add(varname)
        return ast.Name(varname)

    def MAKE_CLOSURE(decompiler, argc):
        if PY2:
            decompiler.stack[-2:-1] = []  # ignore freevars
        else:
            decompiler.stack[-3:-2] = []  # ignore freevars
        return decompiler.MAKE_FUNCTION(argc)

    def MAKE_FUNCTION(decompiler, argc):
        if sys.version_info >= (3, 6):
            if argc:
                if argc != 0x08:
                    throw(NotImplementedError, argc)
            decompiler.stack.pop()
            tos = decompiler.stack.pop()
            if (argc & 0x08):
                decompiler.stack.pop()
        else:
            if argc:
                throw(NotImplementedError)
            tos = decompiler.stack.pop()
            if not PY2:
                tos = decompiler.stack.pop()
        codeobject = tos.value
        func_decompiler = Decompiler(codeobject)
        # decompiler.names.update(decompiler.names)  ???
        if codeobject.co_varnames[:1] == ('.0',):
            return func_decompiler.ast  # generator
        argnames = codeobject.co_varnames[:codeobject.co_argcount]
        defaults = []  # todo
        flags = 0  # todo
        return ast.Lambda(argnames, defaults, flags, func_decompiler.ast)

    def conditional_jump_none_impl(decompiler, endpos, negate):
        expr = decompiler.stack.pop()
        assert(decompiler.pos < decompiler.conditions_end)
        if decompiler.pos in decompiler.or_jumps:
            clausetype = ast.Or
            op = ast.IsNot if negate else ast.Is
        else:
            clausetype = ast.And
            op = ast.Is if negate else ast.IsNot
        expr = ast.Compare(expr, [op()], [ast.Constant(None)])
        decompiler.stack.append(expr)

        if decompiler.next_pos in decompiler.targets:
            decompiler.process_target(decompiler.next_pos)

        expr = decompiler.stack.pop()
        clause = ast.BoolOp(op=clausetype(), values=[expr])
        clause.endpos = endpos
        decompiler.targets.setdefault(endpos, clause)
        return clause

    def jump_if_none(decompiler, endpos):
        return decompiler.conditional_jump_none_impl(endpos, False)

    def jump_if_not_none(decompiler, endpos):
        return decompiler.conditional_jump_none_impl(endpos, True)


    POP_JUMP_BACKWARD_IF_FALSE = JUMP_IF_FALSE
    POP_JUMP_BACKWARD_IF_TRUE = JUMP_IF_TRUE
    POP_JUMP_FORWARD_IF_FALSE = JUMP_IF_FALSE
    POP_JUMP_FORWARD_IF_TRUE = JUMP_IF_TRUE
    POP_JUMP_IF_FALSE = JUMP_IF_FALSE
    POP_JUMP_IF_TRUE = JUMP_IF_TRUE
    POP_JUMP_BACKWARD_IF_NONE = jump_if_none
    POP_JUMP_BACKWARD_IF_NOT_NONE = jump_if_not_none
    POP_JUMP_FORWARD_IF_NONE = jump_if_none
    POP_JUMP_FORWARD_IF_NOT_NONE = jump_if_not_none

    def POP_TOP(decompiler):
        pass

    def RETURN_VALUE(decompiler):
        if decompiler.pos != decompiler.end:
            throw(NotImplementedError)
        expr = decompiler.stack.pop()
        decompiler.stack.append(simplify(expr))
        raise AstGenerated()

    def RETURN_GENERATOR(decompiler):
        pass

    def RESUME(decompiler, where):
        pass

    def ROT_TWO(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        decompiler.stack.append(tos)
        decompiler.stack.append(tos1)

    def ROT_THREE(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack.pop()
        decompiler.stack.append(tos)
        decompiler.stack.append(tos2)
        decompiler.stack.append(tos1)

    def SETUP_LOOP(decompiler, endpos):
        pass

    def SLICE_0(decompiler):
        return ast.Slice(decompiler.stack.pop(), 'OP_APPLY', None, None)

    def SLICE_1(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        return ast.Slice(tos1, 'OP_APPLY', tos, None)

    def SLICE_2(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        return ast.Slice(tos1, 'OP_APPLY', None, tos)

    def SLICE_3(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack.pop()
        return ast.Slice(tos2, 'OP_APPLY', tos1, tos)

    def STORE_ATTR(decompiler, attrname):
        decompiler.store(
            ast.AssAttr(decompiler.stack.pop(), attrname, 'OP_ASSIGN'))

    def STORE_DEREF(decompiler, freevar):
        decompiler.assnames.add(freevar)
        decompiler.store(ast.AssName(freevar, 'OP_ASSIGN'))

    def STORE_FAST(decompiler, varname):
        if varname.startswith('_['):
            throw(InvalidQuery, 'Use generator expression (... for ... in ...) '
                  'instead of list comprehension [... for ... in ...] inside query')  # noqa
        decompiler.assnames.add(varname)
        decompiler.store(ast.AssName(varname, 'OP_ASSIGN'))

    def STORE_MAP(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack[-1]
        if not isinstance(tos2, ast.Dict):
            assert False  # pragma: no cover
        if tos2.items == ():
            tos2.items = []
        tos2.items.append((tos, tos1))

    def STORE_SUBSCR(decompiler):
        tos = decompiler.stack.pop()
        tos1 = decompiler.stack.pop()
        tos2 = decompiler.stack.pop()
        if not isinstance(tos1, ast.Dict):
            assert False  # pragma: no cover
        if tos1.items == ():
            tos1.items = []
        tos1.items.append((tos, tos2))

    def UNARY_POSITIVE(decompiler):
        return ast.UnaryAdd(decompiler.stack.pop())

    def UNARY_NEGATIVE(decompiler):
        return ast.UnarySub(decompiler.stack.pop())

    def UNARY_NOT(decompiler):
        return ast.Not(decompiler.stack.pop())

    def UNARY_CONVERT(decompiler):
        return ast.Backquote(decompiler.stack.pop())

    def UNARY_INVERT(decompiler):
        return ast.Invert(decompiler.stack.pop())

    def UNPACK_SEQUENCE(decompiler, count):
        ass_tuple = ast.AssTuple([])
        ass_tuple.count = count
        return ass_tuple

    def GEN_START(decompiler, kind):
        pass

    def CONTAINS_OP(decompiler, invert):
        return decompiler.COMPARE_OP('not in' if invert else 'in')

    def YIELD_VALUE(decompiler):
        expr = decompiler.stack.pop()
        fors = []
        while decompiler.stack:
            decompiler.process_target(None)
            top = decompiler.stack.pop()
            if not isinstance(top, (ast.GenExprFor)):
                cond = ast.GenExprIf(top)
                top = decompiler.stack.pop()
                assert isinstance(top, ast.GenExprFor)
                top.ifs.append(cond)
                fors.append(top)
            else:
                fors.append(top)
        fors.reverse()
        decompiler.stack.append(
            ast.GenExpr(ast.GenExprInner(simplify(expr), fors)))
        raise AstGenerated()


test_lines = """
    (a and b if c and d else e and f for i in T if (A and B if C and D else E and F))

    (a for b in T)
    (a for b, c in T)
    (a for b in T1 for c in T2)
    (a for b in T1 for c in T2 for d in T3)
    (a for b in T if f)
    (a for b in T if f and h)
    (a for b in T if f and h or t)
    (a for b in T if f == 5 and r or t)
    (a for b in T if f and r and t)

    (a for b in T if f == 5 and +r or not t)
    (a for b in T if -t and ~r or `f`)

    (a**2 for b in T if t * r > y / 3)
    (a + 2 for b in T if t + r > y // 3)
    (a[2,v] for b in T if t - r > y[3])
    ((a + 2) * 3 for b in T if t[r, e] > y[3, r * 4, t])
    (a<<2 for b in T if t>>e > r & (y & u))
    (a|b for c in T1 if t^e > r | (y & (u & (w % z))))

    ([a, b, c] for d in T)
    ([a, b, 4] for d in T if a[4, b] > b[1,v,3])
    ((a, b, c) for d in T)
    ({} for d in T)
    ({'a' : x, 'b' : y} for a, b in T)
    (({'a' : x, 'b' : y}, {'c' : x1, 'd' : 1}) for a, b, c, d in T)
    ([{'a' : x, 'b' : y}, {'c' : x1, 'd' : 1}] for a, b, c, d in T)

    (a[1:2] for b in T)
    (a[:2] for b in T)
    (a[2:] for b in T)
    (a[:] for b in T)
    (a[1:2:3] for b in T)
    (a[1:2, 3:4] for b in T)
    (a[2:4:6,6:8] for a, y in T)

    (a.b.c for d.e.f.g in T)
    # (a.b.c for d[g] in T)

    ((s,d,w) for t in T if (4 != x.a or a*3 > 20) and a * 2 < 5)
    ([s,d,w] for t in T if (4 != x.amount or amount * 3 > 20 or amount * 2 < 5) and amount*8 == 20)
    ([s,d,w] for t in T if (4 != x.a or a*3 > 20 or a*2 < 5 or 4 == 5) and a * 8 == 20)
    (s for s in T if s.a > 20 and (s.x.y == 123 or 'ABC' in s.p.q.r))
    (a for b in T1 if c > d for e in T2 if f < g)

    (func1(a, a.attr, keyarg=123) for s in T)
    (func1(a, a.attr, keyarg=123, *e) for s in T)
    (func1(a, b, a.attr1, a.b.c, keyarg1=123, keyarg2='mx', *e, **f) for s in T)
    (func(a, a.attr, keyarg=123) for a in T if a.method(x, *y, **z) == 4)

    ((x or y) and (p or q) for a in T if (a or b) and (c or d))
    (x.y for x in T if (a and (b or (c and d))) or X)

    (a for a in T1 if a in (b for b in T2))
    (a for a in T1 if a in (b for b in T2 if b == a))

    (a for a in T1 if a in (b for b in T2))
    (a for a in T1 if a in select(b for b in T2))
    (a for a in T1 if a in (b for b in T2 if b in (c for c in T3 if c == a)))
    (a for a in T1 if a > x and a in (b for b in T1 if b < y) and a < z)
"""  # noqa


def test():
    import sys
    from olo.libs.compiler.transformer import parse

    if sys.version[:3] > '2.4':
        outmost_iterable_name = '.0'
    else:
        outmost_iterable_name = '[outmost-iterable]'
    import dis
    for line in test_lines.split('\n'):
        if not line or line.isspace():
            continue
        line = line.strip()
        if line.startswith('#'):
            continue
        code = compile(line, '<?>', 'eval').co_consts[0]
        ast1 = parse(line).node.nodes[0].expr
        ast1.code.quals[0].iter.name = outmost_iterable_name
        try:
            ast2 = Decompiler(code).ast
        except Exception:
            print()
            print(line)
            print()
            print(ast1)
            print()
            dis.dis(code)
            raise
        if str(ast1) != str(ast2):
            print()
            print(line)
            print()
            print(ast1)
            print()
            print(ast2)
            print()
            dis.dis(code)
            break
        else:
            print('OK: %s' % line)
    else:
        print('Done!')


if __name__ == '__main__':
    test()
