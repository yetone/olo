from olo.expression import Expression
from olo.utils import type_checker, Missing
from olo.errors import ParseError
from olo.compat import iteritems


def parse_attrs(cls, attrs, decrypt=True, output=True):
    res = {}
    for k, v in iteritems(attrs):
        if isinstance(v, Missing):
            continue  # pragma: no cover
        # TODO
        if isinstance(v, Expression):
            continue
        field = getattr(cls, k)
        # TODO
        if not hasattr(field, 'type'):
            continue
        if v is not None or not field.noneable:
            if not type_checker(field.type, v):
                v = field.parse(v)
            if not type_checker(field.type, v):
                raise ParseError(
                    'The parsed value of {}.{} is not a {} type: {}. '
                    'Please check the parser of this field'
                    ' or your input data.'.format(
                        cls.__name__, k, field.type, repr(v)
                    )
                )
            if decrypt:
                v = field.decrypt_func(v) if field.encrypt else v
            if output:
                v = field.output(v) if field.output else v
        res[k] = v
    return res


def decrypt_attrs(cls, attrs):
    res = dict(attrs)
    for name, field in iteritems(cls.__encrypted_fields__):
        if name not in res:
            continue  # pragma: no cover
        res[name] = field.decrypt_func(res[name])
    return res
