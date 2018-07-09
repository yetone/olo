from olo.sql_ast_translators.sql_ast_translator import SQLASTTranslator
from olo.context import context, alias_only_context
from olo.utils import compare_operator_precedence, is_sql_ast, car


class MySQLSQLASTTranslator(SQLASTTranslator):
    def post_INSERT(self, *args):
        params = []
        sql_pieces = ['INSERT INTO']
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return ' '.join(sql_pieces), params

    def post_SELECT(self, *args):
        params = []
        sql_pieces = ['SELECT']
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return ' '.join(sql_pieces), params

    def post_UPDATE(self, *args):
        params = []
        sql_pieces = ['UPDATE']
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return ' '.join(sql_pieces), params

    def post_DELETE(self, *args):
        params = []
        sql_pieces = ['DELETE FROM']
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return ' '.join(sql_pieces), params

    def post_SET(self, *args):
        params = []
        sql_pieces = ['SET']
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return ' '.join(sql_pieces), params

    def post_VALUES(self, *args):
        params = []
        sql_pieces = []
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return 'VALUES({})'.format(', '.join(sql_pieces)), params

    def post_MODIFIER(self, modifier, select_ast):
        sql_piece, params = self.translate(select_ast)
        return '{} {}'.format(modifier, sql_piece), params

    def post_BRACKET(self, *args):
        params = []
        sql_pieces = []
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return '({})'.format(', '.join(sql_pieces)), params

    def post_SERIES(self, *args):
        params = []
        sql_pieces = []
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return ', '.join(sql_pieces), params

    def post_COLUMN(self, table_name, field_name):
        if table_name is None:
            return '`{}`'.format(field_name), []
        return '`{}`.`{}`'.format(table_name, field_name), []

    def post_ALIAS(self, raw, alias):
        if context.alias_only:
            return alias, []
        sql_piece, params = self.translate(raw)
        if is_sql_ast(raw) and raw[0] not in ('COLUMN', 'TABLE'):
            sql_piece = '({})'.format(sql_piece)
        return '{} AS {}'.format(sql_piece, alias), params

    def post_FROM(self, arg):
        with alias_only_context(False):  # pylint: disable=E
            sql_piece, params = self.translate(arg)
        return 'FROM {}'.format(sql_piece), params

    def post_JOIN(self, left, right):
        left_sql_piece, left_params = self.translate(left)
        right_sql_piece, right_params = self.translate(right)
        return (
            '{} JOIN {}'.format(left_sql_piece, right_sql_piece),
            left_params + right_params
        )

    def post_LEFT_JOIN(self, left, right):
        left_sql_piece, left_params = self.translate(left)
        right_sql_piece, right_params = self.translate(right)
        return (
            '{} LEFT JOIN {}'.format(left_sql_piece, right_sql_piece),
            left_params + right_params
        )

    def post_RIGHT_JOIN(self, left, right):
        left_sql_piece, left_params = self.translate(left)
        right_sql_piece, right_params = self.translate(right)
        return (
            '{} RIGHT JOIN {}'.format(left_sql_piece, right_sql_piece),
            left_params + right_params
        )

    def post_VALUE(self, value):
        return '%s', [value]

    def post_TABLE(self, table_name):
        return '`{}`'.format(table_name), []

    def post_CALL(self, func_name, *args):
        params = []
        sql_pieces = []
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return (
            '{}({})'.format(
                func_name, ', '.join(map(str, sql_pieces))
            ),
            params
        )

    def post_WHERE(self, exp):
        with alias_only_context(True):  # pylint: disable=E
            sql_piece, params = self.translate(exp)
        return 'WHERE {}'.format(sql_piece), params

    def post_ON(self, exp):
        with alias_only_context(True):  # pylint: disable=E
            sql_piece, params = self.translate(exp)
        return 'ON {}'.format(sql_piece), params

    def post_HAVING(self, exp):
        with alias_only_context(True):  # pylint: disable=E
            sql_piece, params = self.translate(exp)
        return 'HAVING {}'.format(sql_piece), params

    def post_GROUP_BY(self, exp):
        with alias_only_context(True):  # pylint: disable=E
            sql_piece, params = self.translate(exp)
        return 'GROUP BY {}'.format(sql_piece), params

    def post_ORDER_BY(self, exp):
        with alias_only_context(True):  # pylint: disable=E
            sql_piece, params = self.translate(exp)
        return 'ORDER BY {}'.format(sql_piece), params

    def post_LIMIT(self, offset, limit):
        limit_sql_piece, limit_params = self.translate(limit)
        if offset is None:
            return 'LIMIT {}'.format(limit_sql_piece), limit_params
        offset_sql_piece, offset_params = self.translate(offset)
        return (
            'LIMIT {}, {}'.format(offset_sql_piece, limit_sql_piece),
            offset_params + limit_params
        )

    def post_UNARY_OPERATE(self, value, operation):
        with alias_only_context(True):  # pylint: disable=E
            sql_piece, params = self.translate(value)
        return '{} {}'.format(sql_piece, operation), params

    def post_BINARY_OPERATE(self, operator, left, right):
        left_sql_piece, left_params = self.translate(left)
        right_sql_piece, right_params = self.translate(right)
        ops = ('UNARY_OPERATE', 'BINARY_OPERATE')
        if is_sql_ast(left) and car(left) in ops:
            _cmp = compare_operator_precedence(
                left[1], operator
            )
            if _cmp < 0:
                left_sql_piece = '({})'.format(left_sql_piece)  # noqa pragma: no cover
        if is_sql_ast(right):
            right_car = car(right)
            if right_car in ops:
                _cmp = compare_operator_precedence(
                    right[1], operator
                )
                if _cmp < 0:
                    right_sql_piece = '({})'.format(right_sql_piece)  # noqa pragma: no cover
            elif right_car == 'SELECT':
                right_sql_piece = '({})'.format(right_sql_piece)
        return (
            '{} {} {}'.format(
                left_sql_piece, operator, right_sql_piece
            ),
            left_params + right_params
        )

    def post_AND(self, *args):
        params = []
        sql_pieces = []
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return ' AND '.join(sql_pieces), params

    def post_OR(self, *args):
        params = []
        sql_pieces = []
        for x in args:
            sql_piece, _params = self.translate(x)
            sql_pieces.append(sql_piece)
            params.extend(_params)
        return ' OR '.join(sql_pieces), params

    def post_IF(self, test_ast, then_ast, else_ast):
        params = []
        sql_pieces = ['CASE WHEN']
        test_sql_piece, test_params = self.translate(test_ast)
        params.extend(test_params)
        sql_pieces.append(test_sql_piece)
        then_sql_piece, then_params = self.translate(then_ast)
        params.extend(then_params)
        sql_pieces.append('THEN')
        sql_pieces.append(then_sql_piece)
        else_sql_piece, else_params = self.translate(else_ast)
        params.extend(else_params)
        sql_pieces.append('ELSE')
        sql_pieces.append(else_sql_piece)
        sql_pieces.append('END')
        return ' '.join(sql_pieces), params
