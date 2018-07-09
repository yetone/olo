"""abc is too slow"""


class Interface(object):
    pass


class SQLASTInterface(Interface):
    _alias_name = None

    @property
    def alias_name(self):
        return self._alias_name

    def get_sql_ast(self):
        """return sql ast
        @rtype list
        """
        raise NotImplementedError
