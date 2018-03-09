import time
import atexit
import threading
from itertools import chain
from functools import wraps
from Queue import Queue, Empty


class ConnProxy(object):
    def __init__(self, conn, pool, ns):
        self.conn = conn
        self.pool = pool
        self.expire_time = time.time() + pool.timeout
        self.ns = ns
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

    def release(self):
        self.pool.release_conn(self)

    @property
    def is_expired(self):
        return self.expire_time <= time.time()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()


def lock(func):
    @wraps(func)
    def _(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)
    return _


class Pool(object):
    def __init__(self, creator, timeout=60 * 60, queue_size=16,
                 conn_proxy_cls=ConnProxy):
        self.lock = threading.RLock()
        self.creator = creator
        self.conns = {}
        self.flying_conns = set()
        self.timeout = timeout
        self.queue_size = queue_size
        self.conn_proxy_cls = conn_proxy_cls
        atexit.register(self.clear_conns)

    def __str__(self):
        return (
            '<Pool timeout={}, queue_size={}, conn_count={}'
            ', flying_conn_count={}>'
        ).format(
            self.timeout, self.queue_size, self.get_conn_count(),
            len(self.flying_conns)
        )

    __repr__ = __str__

    def get_conn_count(self):
        return sum(q.qsize() for q in self.conns.itervalues())

    @lock
    def get_q(self, ns=''):
        q = self.conns.get(ns)
        if not q:
            q = self.conns[ns] = Queue(maxsize=self.queue_size)
        return q

    def create_conn(self, ns=''):
        conn = self.creator()
        return self.conn_proxy_cls(conn, self, ns)

    @lock
    def get_conn(self, ns=''):
        q = self.get_q(ns=ns)

        try:
            conn = q.get_nowait()

            if conn.is_closed:
                return self.get_conn(ns=ns)

            if conn.is_expired:
                conn.close()
                return self.get_conn(ns=ns)

        except Empty:
            conn = self.create_conn(ns=ns)

        conn.is_release = False
        self.flying_conns.add(conn)

        return conn

    @lock
    def clear_conns(self):
        conns = set()
        for _, v in self.conns.iteritems():
            while not v.empty():
                try:
                    conn = v.get_nowait()
                    conns.add(conn)
                except Empty:
                    pass
        for conn in chain(conns, self.flying_conns):
            conn.close()
        conns.clear()
        self.conns.clear()
        self.flying_conns.clear()

    @lock
    def release_conn(self, conn):
        q = self.get_q(ns=conn.ns)
        if q.full() or conn.is_expired:
            conn.close()
            return
        conn.is_release = True
        q.put_nowait(conn)
        if conn in self.flying_conns:
            self.flying_conns.remove(conn)
