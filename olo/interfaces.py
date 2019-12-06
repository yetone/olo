"""abc is too slow"""
from typing import List


class Interface(object):
    pass


class SQLASTInterface(Interface):
    _alias_name = None

    @property
    def alias_name(self) -> str:
        return self._alias_name

    def get_sql_ast(self) -> List:
        """return sql ast
        @rtype list
        """
        raise NotImplementedError
