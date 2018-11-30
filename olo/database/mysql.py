from threading import Timer

from olo.compat import Empty, Queue

from olo.libs.pool import Pool, ConnProxy
from olo.libs.class_proxy import ClassProxy

from olo.utils import ThreadedObject, parse_execute_sql
from olo.database import BaseDataBase, OLOCursor


def create_conn(host, port, user, password, dbname, charset):
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
        self.pool = Pool(lambda: create_conn(
            host, port, user, password, dbname, charset
        ), conn_proxy_cls=MySQLConnProxy)
        self.modified_cursors = ThreadedObject(Queue)

    def get_conn(self):
        return self.pool.acquire_conn()

    def get_cursor(self):  # pylint: disable=W
        assert self.in_transaction(), 'db.get_cursor must in transaction!'

        tran = self.get_last_transaction()
        conn = tran.conn
        if conn is None:
            conn = tran.conn = self.get_conn()

        cur = conn.cursor()
        return OLOCursor(cur, self)

    def sql_execute(self, sql, params=None, **kwargs):  # pylint: disable=W
        cmd = None
        try:
            cmd, _ = parse_execute_sql(sql)
        except Exception:  # pragma: no cover pylint: disable=W
            pass  # pragma: no cover
        cur = self.get_cursor()
        res = cur.execute(sql, params, **kwargs)
        self.modified_cursors.put_nowait(cur)
        if cmd == 'select':
            return cur.fetchall()
        cur.is_modified = True
        if (
            not kwargs.get('executemany') and
            cmd == 'insert' and cur.lastrowid
        ):
            return cur.lastrowid
        return res

    def sql_commit(self):
        first_err = None
        commited = set()
        while not self.modified_cursors.empty():
            try:
                cur = self.modified_cursors.get_nowait()
                try:
                    if cur.conn not in commited:
                        commited.add(cur.conn)
                        cur.conn.commit()
                        self.log('COMMIT')
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
        rollbacked = set()
        while not self.modified_cursors.empty():
            try:
                cur = self.modified_cursors.get_nowait()
                try:
                    if cur.conn not in rollbacked:
                        rollbacked.add(cur.conn)
                        cur.conn.rollback()
                        self.log('ROLLBACK')
                    cur.is_modified = False
                except Exception as e:  # pragma: no cover pylint: disable=W
                    if first_err is None:  # pragma: no cover
                        first_err = e  # pragma: no cover
            except Empty:  # pragma: no cover
                pass  # pragma: no cover
        if first_err is not None:
            raise first_err  # pragma: no cover pylint: disable=E
