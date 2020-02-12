import re
import types

from olo.libs.compiler.eval import eval_src
from olo.libs.compiler.utils import get_globals_and_locals
from olo.libs.compiler.translators.query_translator import ast2src
from olo.libs.compiler.translators.query_translator2 import ast2factory
from olo.libs.decompiling import decompile
from olo.libs.cache import ic

from olo.errors import GeneratorError


PATTERN_SRC = re.compile(r'^\.0')


class QueryFactory(object):
    key = '___monad___'

    def __init__(self, code_key, tree, globals, locals, cells):
        m = locals['.0']

        model = getattr(m, 'model', None)

        if model is None:
            raise GeneratorError('Need a model class, found: {}'.format(  # noqa pragma: no cover
                repr(m)
            ))

        self.code_key = code_key
        self.tree = tree
        self.globals = globals
        self.locals = locals
        self.cells = cells

        locals[self.key] = model

    def get_query(self):
        src = PATTERN_SRC.sub(
            '{}'.format(self.key),
            ast2src(self.tree)
        )
        return eval_src(src, self.globals, self.locals)


def make_query(args, frame_depth=3):
    gen, globals, locals = get_globals_and_locals(
        args, kwargs=None, frame_depth=frame_depth+1, from_generator=True)
    tree, external_names, cells = decompile(gen)
    code_key = ic.set(gen.gi_frame.f_code)
    return QueryFactory(code_key, tree.code, globals, locals, cells)


def select_(*args):
    return make_query(args).get_query()


def select(*args):
    assert len(args) > 0, 'select take at least one argument!'
    gen, globals, locals = get_globals_and_locals(
        args, kwargs=None, frame_depth=2,
        from_generator=isinstance(args[0], types.GeneratorType))
    tree, external_names, cells = decompile(gen)
    scope = dict(globals)
    scope.update(locals)
    factory = ast2factory(tree.code, scope)
    return factory()
