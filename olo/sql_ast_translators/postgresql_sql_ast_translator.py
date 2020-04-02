from datetime import datetime, date
from enum import Enum

from olo.compat import Decimal, long, unicode
from olo.sql_ast_translators.mysql_sql_ast_translator import MySQLSQLASTTranslator
from olo.types.json import JSONLike
from olo.utils import camel2underscore


class PostgresSQLSQLASTTranslator(MySQLSQLASTTranslator):
    def post_QUOTE(self, value):
        return '"{}"'.format(value), []

    def post_MODIFY_FIELD(self, table_name, name, type_,
                          length, charset, default,
                          noneable, auto_increment, deparse):
        table_name = self.post_QUOTE(table_name)[0]
        f_name = self.post_QUOTE(name)[0]
        type_sql_piece, params = self.post_FIELD_TYPE(type_, length, auto_increment)
        return f'ALTER TABLE {table_name} ALTER COLUMN {f_name} TYPE {type_sql_piece}', params

    def post_FIELD(self, name, type_,
                   length, charset, default,
                   noneable, auto_increment, deparse):
        f_name = self.post_QUOTE(name)[0]
        f_type, params = self.post_FIELD_TYPE(type_, length, auto_increment)

        f_schema = f_type

        if not noneable:
            f_schema += ' NOT NULL'

        f_default, default_params = self.post_FIELD_DEFAULT(f_type, default, noneable, deparse)

        if f_default:
            f_schema += f' DEFAULT {f_default}'
            params += default_params

        return f'{f_name} {f_schema}', params

    def post_FIELD_TYPE(self, type_, length, auto_increment):
        # pylint: disable=too-many-statements
        if type_ in (int, long):
            if auto_increment:
                f_type = 'SERIAL'
            elif length is not None:
                if length < 2:
                    f_type = 'TINYINT'  # pragma: no cover
                elif length < 8:
                    f_type = 'SMALLINT'
                else:
                    f_type = 'INT'
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
        elif type_ is bytes:
            f_type = 'BYTEA'
        elif isinstance(type_, type) and issubclass(type_, Enum):
            f_type = camel2underscore(type_.__name__)
        elif type_ is bool:
            f_type = 'BOOLEAN'
        elif type_ is JSONLike:
            f_type = 'JSONB'
        else:
            f_type = 'TEXT'

        return f_type, []

    def post_RETURNING(self, field_name):
        return f'RETURNING {self.post_QUOTE(field_name)[0]}', []

    def post_TABLE_OPTIONS(self, *options):
        # TODO(postgresql table options)
        return '', []

    def post_KEY(self, type_, name, field_names):
        if type_ == 'PRIMARY':
            return 'PRIMARY KEY ({})'.format(', '.join(
                self.post_QUOTE(x)[0] for x in field_names
            )), []
        if type_ == 'INDEX':
            # TODO
            return '', []
        if type_ == 'UNIQUE':
            return 'CONSTRAINT {} UNIQUE ({})'.format(
                self.post_QUOTE(name)[0],
                ', '.join(self.post_QUOTE(x)[0] for x in field_names)
            ), []
        raise NotImplementedError('key type: {}'.format(type_))

    def post_LIMIT(self, offset, limit):
        limit_sql_piece, limit_params = self.translate(limit)
        if offset is None:
            return 'LIMIT {}'.format(limit_sql_piece), limit_params
        offset_sql_piece, offset_params = self.translate(offset)
        return (
            'LIMIT {} OFFSET {}'.format(offset_sql_piece, limit_sql_piece),
            limit_params + offset_params
        )

    def post_CREATE_ENUM(self, name, labels):
        placeholders = ', '.join(['%s'] * len(labels))
        return f'CREATE TYPE {name} AS ENUM ({placeholders})', labels

    def post_ADD_ENUM_LABEL(self, name, pre_label, label):
        if not pre_label:
            return f'ALTER TYPE {name} ADD VALUE %s', [label]
        return f'ALTER TYPE {name} ADD VALUE %s AFTER %s', [label, pre_label]

    def post_COLUMN(self, table_name, field_name, path=None, type_=None):
        sql, params = super().post_COLUMN(table_name, field_name)
        if type_:
            if type_ != 'text':
                if path:
                    sql = f'({sql} #>> %s)::{type_}'
                else:
                    sql = f'({sql})::{type_}'
            elif path:
                sql = f'({sql} #>> %s)'
        elif path:
            sql = f'({sql} #> %s)'
        if path:
            params.append(path)
        return sql, params
