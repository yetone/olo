from threading import Timer

from olo.database import BaseDataBase, MySQLCursor
from olo.libs.class_proxy import ClassProxy
from olo.libs.pool import Pool, ConnProxy


def create_conn(host, port, user, password, dbname, charset):
    try:
        from MySQLdb import connect
        conn = connect(  # pragma: no cover
            host=host, port=port, user=user, passwd=password, db=dbname,
            charset=charset,
        )
    except ImportError:
        try:
            from pymysql import connect
        except ImportError:  # pragma: no cover
            raise Exception(  # pragma: no cover
                'Cannot found pymsql, please install it: pip install PyMySQL'
            )
        conn = connect(
            host=host, port=port, user=user, password=password, db=dbname,
            charset=charset,
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
        return '<{} {}>'.format(  # pragma: no cover
            self.__class__.__name__,
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

    def ping(self):
        return self.conn.ping()


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
                 report=lambda *args, **kwargs: None,
                 pool_size=5,
                 pool_timeout=30,
                 pool_recycle=60*60,
                 pool_max_overflow=10):

        super(MySQLDataBase, self).__init__(
            beansdb=beansdb,
            autocommit=autocommit,
            report=report
        )
        self.pool = Pool(
            lambda: create_conn(
                host, port, user, password, dbname, charset
            ),
            conn_proxy_cls=MySQLConnProxy,
            size=pool_size,
            timeout=pool_timeout,
            recycle=pool_recycle,
            max_overflow=pool_max_overflow,
        )

    def get_conn(self):
        return self.pool.acquire_conn()

    def get_cursor(self):  # pylint: disable=W
        assert self.in_transaction(), 'db.get_cursor must in transaction!'

        tran = self.get_last_transaction()
        conn = tran.conn
        if conn is None:
            conn = tran.conn = self.get_conn()

        cur = conn.cursor()
        return MySQLCursor(cur, self)
