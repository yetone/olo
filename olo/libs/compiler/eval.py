from olo.libs.compiler.prelude_names import PL_TRANS_FUNC
from olo.libs.cache import LRUCache
from olo.debug import debug


code_cache = LRUCache(128)


def compile_src(src):
    code = code_cache.get(src)
    if not code:
        code = compile(src, '<?>', 'eval')
        code_cache.set(src, code)
    return code


def get_prelude():
    from olo.libs.compiler.translators.func_translator import transform_func
    from olo.funcs import (
        MAX, MIN, COUNT, SUM, AVG, SQRT,
        LENGTH, DISTINCT,
    )
    return {
        PL_TRANS_FUNC: transform_func,
        'max': MAX,
        'min': MIN,
        'count': COUNT,
        'avg': AVG,
        'sum': SUM,
        'sqrt': SQRT,
        'len': LENGTH,
        'distinct': DISTINCT,
    }


def eval_src(src, globals=None, locals=None):
    debug('eval: {}'.format(src))

    globals = globals or {}
    locals = locals or {}
    globals = dict(globals, **get_prelude())
    code = compile_src(src)
    return eval(code, globals, locals)
