class ORMError(Exception):
    pass


class TimeoutError(ORMError):
    pass


class ExpressionError(ORMError):
    pass


class FieldTypeError(ORMError):
    pass


class InvalidFieldError(ORMError):
    pass


class DeparseError(ORMError):
    pass


class ParseError(ORMError):
    pass


class CacheError(ORMError):
    pass


class DataBaseError(ORMError):
    pass


class ValidationError(ORMError):
    pass


class DbFieldVersionError(ORMError):
    pass


class OrderByError(ORMError):
    pass


class GeneratorError(ORMError):
    pass


class SupportError(ORMError):
    pass
