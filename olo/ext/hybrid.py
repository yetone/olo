from olo.expression import BinaryExpression
from olo.utils import missing


class hybrid_property(object):
    def __init__(self, fget, fset=None, fdel=None):
        self.fget = fget
        self._inst_fget = fget
        self.fset = fset
        self.fdel = fdel
        self.__name__ = fget.__name__
        self.__doc__ = fget.__doc__
        self.is_fget_processed = False
        self.value_map = {}

    @property
    def inst_fget(self):
        return self._inst_fget

    @inst_fget.setter
    def inst_fget(self, fget):
        self._inst_fget = fget
        self.is_fget_processed = True

    def set_value(self, cls):
        value = self.fget(cls)
        self.value_map[cls] = value
        if not self.is_fget_processed and isinstance(value, BinaryExpression):
            left = value.left
            right = value.right
            inst_fget = self.inst_fget
            if value.operator == 'IN':
                def inst_fget(self):  # pylint: disable=E0102
                    return getattr(self, left.attr_name) in right
            elif value.operator == 'NOT IN':
                def inst_fget(self):  # pylint: disable=E0102
                    return getattr(self, left.attr_name) not in right
            elif value.operator == 'IS':
                def inst_fget(self):  # pylint: disable=E0102
                    return getattr(self, left.attr_name) is right
            elif value.operator == 'IS NOT':
                def inst_fget(self):  # pylint: disable=E0102
                    return getattr(self, left.attr_name) is not right
            self.inst_fget = inst_fget
        return value

    def __get__(self, obj, objtype):
        if obj is None:
            value = self.value_map.get(objtype, missing)
            if value is missing:
                value = self.set_value(objtype)
            return value
        return self.inst_fget(obj)  # pylint: disable=E1102

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError(
                'Can\'t set this attribute `{}.{}`'.format(
                    obj.__class__.__name, self.__name__
                )
            )
        self.fset(obj, value)

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError(
                'Can\'t delete this attribute `{}.{}`'.format(
                    obj.__class__.__name, self.__name__
                )
            )
        self.fdel(obj)

    def setter(self, fset):
        self.fset = fset
        return self

    def deleter(self, fdel):
        self.fdel = fdel
        return self
