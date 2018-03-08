"""abc is too slow"""


class Interface(object):
    pass


class SQLLiteralInterface(Interface):
    _alias_name = None

    @property
    def alias_name(self):
        return self._alias_name

    def get_sql_and_params(self):
        """return sql literal and params
        @rtype (str, list)
        """
        raise NotImplementedError
