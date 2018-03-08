from Queue import Queue, Empty

from olo.libs.pool import Pool
from olo.libs.class_proxy import ClassProxy

from olo.utils import ThreadedObject, parse_execute_sql
from olo.database import BaseDataBase, OLOCursor


def get_conn(host, port, user, password, dbname, charset):
    try:
        import pymysql
    except ImportError:
        raise Exception(
            'Cannot found pymsql, please install it: pip install PyMySQL'
        )
    conn = pymysql.connect(
        host=host, user=user, password=password, db=dbname,
        charset=charset,
    )
    return conn


class CursorProxy(ClassProxy):
    def __init__(self, raw, conn):
        super(CursorProxy, self).__init__(raw)
        self.conn = conn

    def __iter__(self):
        return iter(self._raw)

    def __str__(self):
        return '<CursorProxy: {}>'.format(self._raw)

    def close(self):
        self.conn.close()
        self._raw.close()

    def execute(self, sql, params=None, **kwargs):
        if (
                params is not None and
                not isinstance(params, (list, tuple, dict))
        ):
            params = (params,)
        return self._raw.execute(sql, params, **kwargs)

    @property
    def is_closed(self):
        return self.conn.is_closed

    @property
    def is_expired(self):
        return self.conn.is_expired


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
        ))
        self.modified_cursors = ThreadedObject(Queue)

    def get_conn(self, table='*'):
        return self.pool.get_conn(ns=table)

    def get_cursor(self, table='*'):
        with self.get_conn(table=table) as conn:
            cur = conn.cursor()
            cur = CursorProxy(cur, conn)
            return OLOCursor(cur, self)

    def sql_execute(self, sql, params=None, **kwargs):
        cmd = None
        table = '*'
        try:
            # FIXME: I don't known why ns must be cost value
            cmd, _ = parse_execute_sql(sql)
        except Exception:
            pass
        cur = self.get_cursor(table=table)
        res = cur.execute(sql, params, **kwargs)
        if cmd == 'select':
            return cur.fetchall()
        self.modified_cursors.put_nowait(cur)
        if (
            not kwargs.get('executemany') and
            cmd == 'insert' and cur.lastrowid
        ):
            return cur.lastrowid
        return res

    def sql_commit(self):
        while not self.modified_cursors.empty():
            try:
                cur = self.modified_cursors.get_nowait()
                if cur.is_closed:
                    continue
                cur.execute('COMMIT')
            except Empty:
                pass

    def sql_rollback(self):
        while not self.modified_cursors.empty():
            try:
                cur = self.modified_cursors.get_nowait()
                if cur.is_closed:
                    continue
                cur.execute('ROLLBACK')
            except Empty:
                pass
