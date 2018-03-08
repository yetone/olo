from contextlib import contextmanager
from .local import Local


class Context(object):
    _local = None

    @property
    def local(self):
        if self._local is None:
            self.__dict__.update({
                '_local': Local()
            })
        return self._local

    def __setattr__(self, k, v):
        setattr(self.local, k, v)

    def __getattr__(self, k):
        return getattr(self.local, k, None)


context = Context()


def switch_context(func):
    ctx_func = contextmanager(func)

    def _(enable=False):
        if enable:
            return ctx_func()

        @contextmanager
        def __():
            try:
                yield
            finally:
                pass

        return __()
    return _


@switch_context
def field_verbose_context():
    _field_verbose = context.field_verbose
    try:
        context.field_verbose = True
        yield
    finally:
        context.field_verbose = _field_verbose


@contextmanager
def model_instantiate_context(ctx):
    _ctx = ctx.in_model_instantiate
    _depth = ctx.instantiate_depth or 0
    try:
        ctx.in_model_instantiate = True
        ctx.instantiate_depth = _depth + 1
        yield
    finally:
        ctx.in_model_instantiate = _ctx
        ctx.instantiate_depth = _depth
