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


@contextmanager
def alias_only_context(alias_only=True):
    _alias_only = context.alias_only
    try:
        context.alias_only = alias_only
        yield
    finally:
        context.alias_only = _alias_only


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


@contextmanager
def table_alias_mapping_context(alias_mapping):
    _alias_mapping = context.table_alias_mapping
    try:
        context.table_alias_mapping = alias_mapping
        yield
    finally:
        context.table_alias_mapping = _alias_mapping
