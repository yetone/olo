import time
import atexit
import threading
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


class Pool(object):
    def __init__(self, creator, timeout=60 * 60, queue_size=16):
        self.lock = threading.RLock()
        self.creator = creator
        self.conns = {}
        self.timeout = timeout
        self.queue_size = queue_size
        atexit.register(self.clear_conns)

    def get_q(self, ns=''):
        with self.lock:
            q = self.conns.get(ns)
            if not q:
                q = self.conns[ns] = Queue(maxsize=self.queue_size)
        return q

    def create_conn(self, ns=''):
        conn = self.creator()
        return ConnProxy(conn, self, ns)

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

        return conn

    def clear_conns(self):
        with self.lock:
            for _, v in self.conns.items():
                while not v.empty():
                    try:
                        conn = v.get_nowait()
                        conn.close()
                    except Empty:
                        pass
            self.conns.clear()

    def release_conn(self, conn):
        q = self.get_q(ns=conn.ns)
        if q.full() or conn.is_expired:
            conn.close()
            return
        conn.is_release = True
        q.put_nowait(conn)
