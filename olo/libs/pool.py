import time
import weakref
import threading


class ConnProxy(object):
    def __init__(self, conn, pool):
        self.conn = conn
        self.pool = pool
        self.expire_time = time.time() + pool.timeout
        self.is_closed = False
        self.is_release = False

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __getattr__(self, item):
        return getattr(self.conn, item)

    def __str__(self):
        return '<ConnProxy conn={}, pool={}>'.format(
            self.conn, self.pool
        )

    __repr__ = __str__

    def close(self):
        self.conn.close()
        self.is_closed = True

    @property
    def is_expired(self):
        return self.expire_time <= time.time()


class Pool(object):
    def __init__(self, creator, timeout=60 * 60,
                 conn_proxy_cls=ConnProxy):
        self.local = threading.local()
        self.reset_local()
        self.creator = creator
        self.timeout = timeout
        self.conn_proxy_cls = conn_proxy_cls

    def __str__(self):
        return (
            '<Pool timeout={}>'
        ).format(self.timeout)

    __repr__ = __str__

    def reset_local(self):
        self.local.conn = lambda: None

    def create_conn(self):
        conn = self.creator()
        return self.conn_proxy_cls(conn, self)

    def get_conn(self):
        conn = self.local.conn()

        if not conn:
            conn = self.create_conn()
            self.local.conn = weakref.ref(conn)

        if conn.is_closed:
            self.reset_local()
            return self.get_conn()

        if conn.is_expired:
            conn.close()
            self.reset_local()
            return self.get_conn()

        return conn

    def clear_conns(self):
        conn = self.get_conn()
        conn.close()
        self.reset_local()
