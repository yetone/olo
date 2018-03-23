from threading import Timer
from Queue import Queue, Empty

from olo.libs.pool import Pool, ConnProxy
from olo.libs.class_proxy import ClassProxy

from olo.utils import ThreadedObject, parse_execute_sql
from olo.database import BaseDataBase, OLOCursor


def get_conn(host, port, user, password, dbname, charset):
    try:
        from MySQLdb import connect
        conn = connect(
            host=host, user=user, passwd=password, db=dbname,
            charset=charset,
        )
    except ImportError:
        from pymysql import connect
        conn = connect(
            host=host, user=user, password=password, db=dbname,
            charset=charset,
        )
    except ImportError:
        raise Exception(
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
        return '<MySQLConnProxy {}>'.format(
            super(MySQLConnProxy, self).__str__()
        )

    def cursor(self):
        cur = self.conn.cursor()
        cur = CursorProxy(cur, self)
        return cur

    def close(self):
        if self.modified_cursors:
            self.waiting_for_close = True
            Timer(60, self._close).start()
            return
        self.waiting_for_close = False
        self._close()

    def _close(self):
        super(MySQLConnProxy, self).close()
        for cur in self.modified_cursors:
            cur.close()
        self.modified_cursors.clear()

    def add_modified_cursor(self, cur):
        self.modified_cursors.add(cur)

    def remove_modified_cursor(self, cur):
        if cur in self.modified_cursors:
            self.modified_cursors.remove(cur)
            if self.waiting_for_close:
                self.close()


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
        return '<CursorProxy {}>'.format(self._raw)

    def close(self):
        self._raw.close()
        if not self.conn.is_closed:
            self.conn.remove_modified_cursor(self)

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

    def sql_execute(self, sql, params=None, **kwargs):  # pylint: disable=W
        cmd = None
        try:
            cmd, _ = parse_execute_sql(sql)
        except Exception:  # pylint: disable=W
            pass
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
                except Exception as e:  # pylint: disable=W
                    if first_err is None:
                        first_err = e
            except Empty:
                pass
        if first_err is not None:
            raise first_err  # pylint: disable=E

    def sql_rollback(self):
        first_err = None
        while not self.modified_cursors.empty():
            try:
                cur = self.modified_cursors.get_nowait()
                try:
                    cur.conn.rollback()
                    cur.is_modified = False
                except Exception as e:  # pylint: disable=W
                    if first_err is None:
                        first_err = e
            except Empty:
                pass
        if first_err is not None:
            raise first_err  # pylint: disable=E
