from olo.utils import car, cdr, is_sql_ast


class SQLASTTranslator(object):
    def translate(self, sql_ast):
        if not is_sql_ast(sql_ast):
            return sql_ast, []

        head = car(sql_ast)
        tail = cdr(sql_ast)
        method_name = 'post_{}'.format('_'.join(head.split(' ')))
        method = getattr(self, method_name, None)
        if method is None:
            raise NotImplementedError(method_name)
        return method(*tail)  # pylint: disable=not-callable
