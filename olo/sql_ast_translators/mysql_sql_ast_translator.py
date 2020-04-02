import json
from datetime import datetime, date
from enum import Enum

from olo.sql_ast_translators.sql_ast_translator import SQLASTTranslator
from olo.context import context, alias_only_context
from olo.utils import compare_operator_precedence, is_sql_ast, car
from olo.compat import (
    Decimal, long, unicode,
)


class MySQLSQLASTTranslator(SQLASTTranslator):
    def post_PROGN(self, *args):
        sql_pieces, params = self.reduce(
            ['SENTENCE', a] for a in args
        )
        return '\n'.join(sql_pieces), params

    def post_SENTENCE(self, ast):
        sql_piece, params = self.translate(ast)
        return '{};'.format(sql_piece), params

    def post_INSERT(self, *args):
        sql_pieces, params = self.reduce(args)
        sql_pieces.insert(0, 'INSERT INTO')
        return ' '.join(sql_pieces), params

    def post_SELECT(self, *args):
        sql_pieces, params = self.reduce(args)
        sql_pieces.insert(0, 'SELECT')
        return ' '.join(sql_pieces), params

    def post_UPDATE(self, *args):
        sql_pieces, params = self.reduce(args)
        sql_pieces.insert(0, 'UPDATE')
        return ' '.join(sql_pieces), params

    def post_DELETE(self, *args):
        sql_pieces, params = self.reduce(args)
        sql_pieces.insert(0, 'DELETE FROM')
        return ' '.join(sql_pieces), params

    def post_SET(self, *args):
        sql_pieces, params = self.reduce(args)
        sql_pieces.insert(0, 'SET')
        return ' '.join(sql_pieces), params

    def post_VALUES(self, *args):
        sql_pieces, params = self.reduce(args)
        return 'VALUES({})'.format(', '.join(sql_pieces)), params

    def post_MODIFIER(self, modifier, select_ast):
        sql_piece, params = self.translate(select_ast)
        return '{} {}'.format(modifier, sql_piece), params

    def post_BRACKET(self, *args):
        sql_pieces, params = self.reduce(args)
        return '({})'.format(', '.join(sql_pieces)), params

    def post_SERIES(self, *args):
        sql_pieces, params = self.reduce(args)
        return ', '.join(sql_pieces), params

    def post_COLUMN(self, table_name, field_name, path=None, type_=None):
        if table_name is None:
            return self.post_QUOTE(field_name)
        alias_mapping = context.table_alias_mapping or {}
        table_name = alias_mapping.get(table_name, table_name)
        sql = '{}.{}'.format(self.post_QUOTE(table_name)[0], self.post_QUOTE(field_name)[0])
        params = []
        return sql, params

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

    def post_JOIN(self, join_type, left_ast, right_ast, on_ast):
        left_sql_piece, left_params = self.translate(left_ast)
        right_sql_piece, right_params = self.translate(right_ast)
        if not on_ast:
            return (
                f'{left_sql_piece} {join_type} JOIN {right_sql_piece}',
                left_params + right_params
            )
        on_sql_piece, on_params = self.translate(on_ast)
        return (
            f'{left_sql_piece} {join_type} JOIN {right_sql_piece} ON {on_sql_piece}',
            left_params + right_params + on_params
        )

    def post_VALUE(self, value):
        return '%s', [value]

    def post_QUOTE(self, value):
        return '`{}`'.format(value), []

    def post_TABLE(self, table_name):
        return self.post_QUOTE(table_name)

    def post_RETURNING(self, field_name):
        return '', []

    def post_CALL(self, func_name, *args):
        sql_pieces, params = self.reduce(args)
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

    def post_UNARY_OPERATE(self, value, operation, suffix=True):
        with alias_only_context(True):  # pylint: disable=E
            sql_piece, params = self.translate(value)
        if suffix:
            return '{} {}'.format(sql_piece, operation), params
        return '{} ({})'.format(operation, sql_piece), params

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
        sql_pieces, params = self.reduce(args)
        return ' AND '.join(sql_pieces), params

    def post_OR(self, *args):
        sql_pieces, params = self.reduce(args)
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

    def post_CREATE_INDEX(self, if_not_exists, idx_name, tbl_name, field_names):
        sql = (
            f"CREATE INDEX {'IF NOT EXISTS ' if if_not_exists else ''} {self.post_QUOTE(idx_name)[0]} "
            f"ON {self.post_TABLE(tbl_name)[0]} ({', '.join(self.post_QUOTE(name)[0] for name in field_names)})"
        )
        return sql, []

    def post_CREATE_TABLE(self, tmp, if_not_exists, tbl_name,
                          create_definition_ast, tbl_options_ast):
        sql_piece = 'CREATE '
        if tmp:
            sql_piece += 'TEMPORARY '  # pragma: no cover
        sql_piece += 'TABLE '
        if if_not_exists:
            sql_piece += 'IF NOT EXISTS '
        sql_piece += self.post_TABLE(tbl_name)[0]
        _sql_piece, params = self.translate(create_definition_ast)
        sql_piece = sql_piece + '(\n' + _sql_piece + '\n)'
        _sql_piece, _params = self.translate(tbl_options_ast)
        params.extend(_params)
        sql_piece += ' ' + _sql_piece
        return sql_piece, params

    def post_CREATE_ENUM(self, name, labels):
        return '', []

    def post_ADD_ENUM_LABEL(self, name, pre_label, label):
        return '', []

    def post_CREATE_DEFINITION(self, *args):
        sql_pieces, params = self.reduce(args)
        return ',\n'.join('  {}'.format(p) for p in sql_pieces if p), params

    def post_MODIFY_FIELD(self, table_name, name, type_,
                          length, charset, default,
                          noneable, auto_increment, deparse):
        sql_piece, params = self.post_FIELD(name, type_, length, charset, default, noneable, auto_increment, deparse)
        table_name = self.post_QUOTE(table_name)[0]
        return f'ALTER TABLE {table_name} MODIFY {sql_piece}', params

    def post_ADD_FIELD(self, table_name, name, type_,
                       length, charset, default,
                       noneable, auto_increment, deparse):
        sql_piece, params = self.post_FIELD(name, type_, length, charset, default, noneable, auto_increment, deparse)
        table_name = self.post_QUOTE(table_name)[0]
        return f'ALTER TABLE {table_name} ADD {sql_piece}', params

    def post_FIELD(self, name, type_,
                   length, charset, default,
                   noneable, auto_increment, deparse):
        f_name = self.post_QUOTE(name)[0]
        f_type, params = self.post_FIELD_TYPE(type_, length, auto_increment)

        f_schema = f_type
        if charset is not None:
            f_schema += ' CHARACTER SET {}'.format(charset)

        if not noneable:
            f_schema += ' NOT NULL'

        f_default, default_params = self.post_FIELD_DEFAULT(f_type, default, noneable, deparse)

        if f_default:
            f_schema += f' DEFAULT {f_default}'
            params += default_params

        if auto_increment:
            f_schema += ' AUTO_INCREMENT'

        return f'{f_name} {f_schema}', params

    def post_FIELD_TYPE(self, type_, length, auto_increment):
        # pylint: disable=too-many-statements
        if type_ in (int, long):
            if length is not None:
                if length < 2:
                    f_type = 'TINYINT({})'.format(length)  # pragma: no cover
                elif length < 8:
                    f_type = 'SMALLINT({})'.format(length)
                else:
                    f_type = 'INT({})'.format(length)
            else:
                f_type = 'BIGINT'
        elif type_ in (str, unicode):
            if length is not None:
                f_type = 'VARCHAR({})'.format(length)
            else:
                f_type = 'TEXT'  # pragma: no cover
        elif type_ is float:
            f_type = 'FLOAT'  # pragma: no cover
        elif type_ is Decimal:
            f_type = 'DECIMAL'  # pragma: no cover
        elif type_ is date:
            f_type = 'DATE'
        elif type_ is datetime:
            f_type = 'TIMESTAMP'
        elif isinstance(type_, type) and issubclass(type_, Enum):
            f_type = 'VARCHAR(128)'
        elif type_ is bool:
            f_type = 'BOOLEAN'
        else:
            f_type = 'TEXT'

        return f_type, []

    def post_FIELD_DEFAULT(self, f_type, default,
                           noneable, deparse):

        f_default = ''
        params = []
        if f_type not in (
                'BLOB', 'GEOMETRY'
        ):
            if not callable(default):
                if default is not None or noneable:
                    if default is not None:
                        f_default = '%s'
                        if f_type == 'JSONB':
                            f_default = '%s::jsonb'
                        if f_type in ('TEXT', 'JSONB'):
                            params.append(json.dumps(default))
                        elif isinstance(default, Enum):
                            params.append(default.name)
                        else:
                            params.append(default)
                    else:
                        f_default = 'NULL'
            elif default == datetime.now:
                f_default = 'CURRENT_TIMESTAMP'

        return f_default, params

    def post_KEY(self, type_, name, field_names):
        if type_ == 'PRIMARY':
            return 'PRIMARY KEY ({})'.format(', '.join(
                self.post_QUOTE(x)[0] for x in field_names
            )), []
        if type_ == 'INDEX':
            return 'INDEX {} ({})'.format(
                self.post_QUOTE(name)[0],
                ', '.join(self.post_QUOTE(x)[0] for x in field_names)
            ), []
        if type_ == 'UNIQUE':
            return 'UNIQUE KEY {} ({})'.format(
                self.post_QUOTE(name)[0],
                ', '.join(self.post_QUOTE(x)[0] for x in field_names)
            ), []
        raise NotImplementedError('key type: {}'.format(type_))

    def post_TABLE_OPTIONS(self, *options):
        return ' '.join('{}={}'.format(x[0], x[1]) for x in options), []

    def post_FOR_UPDATE(self):
        return 'FOR UPDATE', []
