from datetime import datetime, date
from enum import Enum

from olo.compat import Decimal, long, unicode
from olo.sql_ast_translators.mysql_sql_ast_translator import MySQLSQLASTTranslator
from olo.utils import camel2underscore


class PostgresSQLSQLASTTranslator(MySQLSQLASTTranslator):
    def post_QUOTE(self, value):
        return '"{}"'.format(value), []

    def post_FIELD(self, name, type_,
                   length, charset, default,
                   noneable, auto_increment, deparse):
        # pylint: disable=too-many-statements
        f_schema = self.post_QUOTE(name)[0]
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
        elif isinstance(type_, type) and issubclass(type_, Enum):
            f_type = camel2underscore(type_.__name__)
        else:
            f_type = 'TEXT'

        f_schema += ' ' + f_type
        if charset is not None:
            # TODO
            # f_schema += ' CHARACTER SET {}'.format(charset)
            pass

        if not noneable:
            f_schema += ' NOT NULL'

        if f_type not in (
                'BLOB', 'TEXT', 'GEOMETRY', 'JSON'
        ):
            if not callable(default):
                if default is not None or noneable:
                    if default is not None:
                        f_schema += ' DEFAULT \'{}\''.format(
                            deparse(default)
                        )
                    else:
                        f_schema += ' DEFAULT NULL'
            elif default == datetime.now:
                f_schema += ' DEFAULT CURRENT_TIMESTAMP'

        return f_schema, []

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
