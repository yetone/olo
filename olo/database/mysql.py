from threading import Timer
from datetime import date, datetime

from olo.compat import Queue, Empty, long, unicode, Decimal

from olo.libs.pool import Pool, ConnProxy
from olo.libs.class_proxy import ClassProxy

from olo.utils import ThreadedObject, parse_execute_sql, to_camel_case
from olo.database import BaseDataBase, OLOCursor
from olo.field import BaseField


def get_conn(host, port, user, password, dbname, charset):
    try:
        from MySQLdb import connect
        conn = connect(  # pragma: no cover
            host=host, user=user, passwd=password, db=dbname,
            charset=charset,
        )
    except ImportError:
        from pymysql import connect
        conn = connect(
            host=host, user=user, password=password, db=dbname,
            charset=charset,
        )
    except ImportError:  # pragma: no cover
        raise Exception(  # pragma: no cover
            'Cannot found pymsql, please install it: pip install PyMySQL'
        )
    return conn


class MySQLConnProxy(ConnProxy):
    def __init__(self, conn, pool):
        super(MySQLConnProxy, self).__init__(
            conn, pool
        )
        self.modified_cursors = set()
        self.waiting_for_close = False

    def __str__(self):
        return '<MySQLConnProxy {}>'.format(  # pragma: no cover
            super(MySQLConnProxy, self).__str__()
        )

    def cursor(self):
        cur = self.conn.cursor()
        cur = CursorProxy(cur, self)
        return cur

    def close(self):
        if self.modified_cursors:
            self.waiting_for_close = True  # pragma: no cover
            Timer(60, self._close).start()  # pragma: no cover
            return  # pragma: no cover
        self.waiting_for_close = False
        self._close()

    def _close(self):
        super(MySQLConnProxy, self).close()
        for cur in self.modified_cursors:
            cur.close()  # pragma: no cover
        self.modified_cursors.clear()

    def add_modified_cursor(self, cur):
        self.modified_cursors.add(cur)

    def remove_modified_cursor(self, cur):
        if cur in self.modified_cursors:
            self.modified_cursors.remove(cur)
            if self.waiting_for_close:
                self.close()  # pragma: no cover


class CursorProxy(ClassProxy):
    def __init__(self, raw, conn):
        super(CursorProxy, self).__init__(raw)
        self.conn = conn
        self._is_modified = False

    @property
    def is_modified(self):
        return self._is_modified

    @is_modified.setter
    def is_modified(self, item):
        self._is_modified = item
        if self.is_modified:
            self.conn.add_modified_cursor(self)
        else:
            self.conn.remove_modified_cursor(self)

    def __iter__(self):
        return iter(self._raw)

    def __str__(self):
        return '<CursorProxy {}>'.format(self._raw)  # pragma: no cover

    def close(self):
        self._raw.close()  # pragma: no cover
        if not self.conn.is_closed:  # pragma: no cover
            self.conn.remove_modified_cursor(self)  # pragma: no cover

    def execute(self, sql, params=None, **kwargs):
        if (
                params is not None and
                not isinstance(params, (list, tuple, dict))
        ):
            params = (params,)
        return self._raw.execute(sql, params, **kwargs)


class MySQLDataBase(BaseDataBase):
    def __init__(self, host, port, user, password, dbname,
                 charset='utf8mb4',
                 beansdb=None, autocommit=True,
                 report=lambda *args, **kwargs: None):

        super(MySQLDataBase, self).__init__(
            beansdb=beansdb,
            autocommit=autocommit,
            report=report
        )
        self.pool = Pool(lambda: get_conn(
            host, port, user, password, dbname, charset
        ), conn_proxy_cls=MySQLConnProxy)
        self.modified_cursors = ThreadedObject(Queue)

    def get_conn(self):
        return self.pool.get_conn()

    def get_cursor(self):  # pylint: disable=W
        conn = self.get_conn()
        cur = conn.cursor()
        return OLOCursor(cur, self)

    @classmethod
    def to_model_table_schema(cls, model):
        # pylint: disable=too-many-statements

        field_schemas = []
        for k in model.__sorted_fields__:
            f = getattr(model, k)

            f_schema = '`{}`'.format(f.name)

            f_type = ''
            if f.type in (int, long):
                if f.length is not None:
                    if f.length < 2:
                        f_type = 'TINYINT({})'.format(f.length)  # pragma: no cover
                    elif f.length < 8:
                        f_type = 'SMALLINT({})'.format(f.length)
                    else:
                        f_type = 'INT({})'.format(f.length)
                else:
                    f_type = 'INT'
            elif f.type in (str, unicode):
                if f.length is not None:
                    f_type = 'VARCHAR({})'.format(f.length)
                else:
                    f_type = 'TEXT'  # pragma: no cover
            elif f.type is float:
                f_type = 'FLOAT'  # pragma: no cover
            elif f.type is Decimal:
                f_type = 'DECIMAL'  # pragma: no cover
            elif f.type is date:
                f_type = 'DATE'
            elif f.type is datetime:
                f_type = 'TIMESTAMP'
            else:
                f_type = 'TEXT'

            f_schema += ' ' + f_type
            if f.charset is not None:
                f_schema += ' CHARACTER SET {}'.format(f.charset)
            if not f.noneable:
                f_schema += ' NOT NULL'

            if f_type not in (
                    'BLOB', 'TEXT', 'GEOMETRY', 'JSON'
            ):
                if not callable(f.default):
                    if f.default is not None or f.noneable:
                        if f.default is not None:
                            f_schema += ' DEFAULT \'{}\''.format(
                                f.deparse(f.default)
                            )
                        else:
                            f_schema += ' DEFAULT NULL'
                elif f.default == datetime.now:
                    f_schema += ' DEFAULT CURRENT_TIMESTAMP'

            if f.auto_increment:
                f_schema += ' AUTO_INCREMENT'

            field_schemas.append(f_schema)

        field_schemas.append('PRIMARY KEY ({})'.format(', '.join(
            '`{}`'.format(x) for x in model.__primary_key__
        )))

        for key in model.__index_keys__:
            key_schema = 'INDEX'
            key_name = 'idx_' + '_'.join(map(to_camel_case, key))
            key_schema += ' `{}`'.format(key_name)
            names = []
            for p in key:
                f = getattr(model, p)
                if not isinstance(f, BaseField):
                    break  # pragma: no cover
                names.append(f.name)
            else:
                if not names:
                    continue
                key_schema += ' ({})'.format(
                    ', '.join('`{}`'.format(x) for x in names)
                )
                field_schemas.append(key_schema)

        for key in model.__unique_keys__:
            key_schema = 'UNIQUE KEY'
            key_name = 'uk_' + '_'.join(map(to_camel_case, key))
            key_schema += ' `{}`'.format(key_name)
            names = []
            for p in key:
                f = getattr(model, p)
                if not isinstance(f, BaseField):
                    break  # pragma: no cover
                names.append(f.name)
            else:
                if not names:
                    continue  # pragma: no cover
                key_schema += ' ({})'.format(
                    ', '.join('`{}`'.format(x) for x in names)
                )
                field_schemas.append(key_schema)

        schema = 'CREATE TABLE IF NOT EXISTS `{}` (\n'.format(
            model.__table_name__
        )
        schema += ',\n'.join('  ' + f for f in field_schemas)
        schema += '\n)'
        if model._options.table_engine is not None:
            schema += ' ENGINE={}'.format(model._options.table_engine)
        if model._options.table_charset is not None:
            schema += ' DEFAULT CHARSET={}'.format(
                model._options.table_charset
            )
        return schema + ';'

    def gen_tables_schema(self):
        schemas = [
            self.to_model_table_schema(m)
            for m in self._models
        ]
        return '\n\n'.join(schemas)

    def sql_execute(self, sql, params=None, **kwargs):  # pylint: disable=W
        cmd = None
        try:
            cmd, _ = parse_execute_sql(sql)
        except Exception:  # pragma: no cover pylint: disable=W
            pass  # pragma: no cover
        cur = self.get_cursor()
        res = cur.execute(sql, params, **kwargs)
        if cmd == 'select':
            return cur.fetchall()
        cur.is_modified = True
        self.modified_cursors.put_nowait(cur)
        if (
            not kwargs.get('executemany') and
            cmd == 'insert' and cur.lastrowid
        ):
            return cur.lastrowid
        return res

    def sql_commit(self):
        first_err = None
        while not self.modified_cursors.empty():
            try:
                cur = self.modified_cursors.get_nowait()
                try:
                    cur.conn.commit()
                    cur.is_modified = False
                except Exception as e:  # pragma: no cover pylint: disable=W
                    if first_err is None:  # pragma: no cover
                        first_err = e  # pragma: no cover
            except Empty:  # pragma: no cover
                pass  # pragma: no cover
        if first_err is not None:
            raise first_err  # pragma: no cover pylint: disable=E

    def sql_rollback(self):
        first_err = None
        while not self.modified_cursors.empty():
            try:
                cur = self.modified_cursors.get_nowait()
                try:
                    cur.conn.rollback()
                    cur.is_modified = False
                except Exception as e:  # pragma: no cover pylint: disable=W
                    if first_err is None:  # pragma: no cover
                        first_err = e  # pragma: no cover
            except Empty:  # pragma: no cover
                pass  # pragma: no cover
        if first_err is not None:
            raise first_err  # pragma: no cover pylint: disable=E
