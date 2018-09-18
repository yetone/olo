# pylint: disable=E1121

import io
import sys
import types

from olo.compat import imap, pickle, str_types


def throw(exp, *args):
    raise exp(*args)


def _persistent_id(obj):
    if obj is Ellipsis:
        return 'Ellipsis'


def _persistent_load(persid):
    if persid == 'Ellipsis':
        return Ellipsis
    raise pickle.UnpicklingError('unsupported persistent object')


def pickle_ast(val):
    pickled = io.BytesIO()
    pickler = pickle.Pickler(pickled)
    pickler.persistent_id = _persistent_id
    pickler.dump(val)
    return pickled


def unpickle_ast(pickled):
    pickled.seek(0)
    unpickler = pickle.Unpickler(pickled)
    unpickler.persistent_load = _persistent_load
    return unpickler.load()


def copy_ast(tree):
    return unpickle_ast(pickle_ast(tree))


def get_globals_and_locals(args, kwargs, frame_depth, from_generator=False):
    args_len = len(args)
    assert args_len > 0
    func = args[0]
    if from_generator:
        if not isinstance(func, str_types + (types.GeneratorType,)):
            throw(
                TypeError,
                'The first positional argument must be generator expression or'
                ' its text source. Got: %r' % func
            )
    else:
        if not isinstance(func, str_types + (types.FunctionType,)):
            throw(
                TypeError,
                'The first positional argument must be lambda function or'
                ' its text source. Got: %r' % func
            )
    if args_len > 1:
        globals = args[1]
        if not hasattr(globals, 'keys'):
            throw(
                TypeError,
                'The second positional arguments should be globals dictionary.'
                ' Got: %r' % globals
            )
        if args_len > 2:
            locals = args[2]
            if not hasattr(locals, 'keys'):
                throw(
                    TypeError,
                    'The third positional arguments should be locals'
                    ' dictionary. Got: %r' % locals
                )
        else:
            locals = {}
        if isinstance(func, types.GeneratorType):
            locals = dict(locals, **func.gi_frame.f_locals)
        if len(args) > 3:
            throw(
                TypeError,
                'Excess positional argument%s: %s' % (
                    len(args) > 4 and 's' or '',
                    ', '.join(imap(repr, args[3:])))
            )
    else:
        locals = {}
        frame = sys._getframe(frame_depth + 1)
        locals.update(frame.f_locals)
        if isinstance(func, types.GeneratorType):
            globals = func.gi_frame.f_globals
            locals.update(func.gi_frame.f_locals)
        elif isinstance(func, types.FunctionType):
            globals = dict(func.__globals__)
        else:
            globals = frame.f_globals
    if kwargs:
        throw(
            TypeError,
            'Keyword arguments cannot be specified together with positional'
            ' arguments'
        )
    return func, globals, locals
