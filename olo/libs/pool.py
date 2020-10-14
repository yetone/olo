import logging
import sys
import time
import threading
from datetime import datetime

from olo.libs.queue import Queue, Empty, Full
from olo.logger import logger
from olo.utils import log_call


def log_pool(fmt):

    return log_call(
        '[POOL]: {}'.format(fmt),
        logger,
        level=logging.DEBUG,
        toggle=(
            lambda *args, **kwargs:
            len(args) > 0 and getattr(args[0], '_enable_log', False)
        )
    )


class ConnProxy(object):
    lock = threading.RLock()
    pid = 0

    def __init__(self, conn, pool: 'Pool'):
        with self.lock:
            if self.__class__.pid >= sys.maxsize:
                self.__class__.pid = 0
            self.__class__.pid += 1
            self.id = self.__class__.pid
        self.conn = conn
        self.pool = pool
        self.expire_time = time.time() + pool._recycle
        self.is_closed = False
        self.is_released = False
        self.created_at = datetime.now()

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __getattr__(self, item):
        return getattr(self.conn, item)

    def __str__(self):
        return '<ConnProxy id={}, created_at={}, conn={}, pool={}>'.format(
            self.id, self.created_at.strftime('%Y-%m-%d %H:%M:%S'), self.conn, self.pool
        )

    __repr__ = __str__

    def close(self):
        self.release()

    def do_real_close(self):
        self.is_closed = True
        self.conn.close()

    @property
    def is_expired(self):
        return self.expire_time <= time.time()

    def release(self):
        self.pool.release_conn(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.release()

    def ping(self):
        raise NotImplementedError


class Pool(object):
    def __init__(self,
                 creator,
                 size=5,
                 max_overflow=10,
                 timeout=30,
                 recycle=60 * 60,
                 tick_time=0.01,
                 conn_proxy_cls=ConnProxy,
                 use_lifo=False,
                 enable_log=False):
        self._creator = creator
        self._pool = Queue(size, use_lifo=use_lifo)
        self._overflow = 0 - size
        self._max_overflow = max_overflow
        self._timeout = timeout
        self._recycle = recycle
        self._overflow_lock = threading.RLock()
        self._tick_time = tick_time
        self._timeout = timeout
        self._conn_proxy_cls = conn_proxy_cls
        self._enable_log = enable_log

    def __str__(self):
        return (
            '<Pool size={}, idle_size={}, overflow={}, max_overflow={}>'
        ).format(
            self.size, self.idle_size, self._overflow, self._max_overflow)

    __repr__ = __str__

    def _inc_overflow(self):
        if self._max_overflow == -1:
            self._overflow += 1
            return True
        with self._overflow_lock:
            if self._overflow < self._max_overflow:
                self._overflow += 1
                return True
            else:
                return False

    def _dec_overflow(self):
        if self._max_overflow == -1:
            self._overflow -= 1
            return True
        with self._overflow_lock:
            self._overflow -= 1
            return True

    @property
    def size(self):
        return self._pool.maxsize

    @property
    def idle_size(self):
        return self._pool.qsize()

    def _create_conn(self):
        conn = self._creator()
        return self._conn_proxy_cls(conn, self)

    @log_pool('acquire conn: {%ret}')
    def acquire_conn(self):
        while True:
            conn = self._do_acquire_conn()
            if conn is None:
                raise Exception(f'cannot connect to the database!!!')
            if conn.is_expired or conn.is_closed or not self.ping_conn(conn):
                try:
                    self.destroy_conn(conn)
                except Exception:
                    pass
                continue
            conn.is_released = False
            return conn

    def _do_acquire_conn(self):
        use_overflow = self._max_overflow > -1

        try:
            wait = use_overflow and self._overflow >= self._max_overflow
            return self._pool.get(wait, self._timeout)
        except Empty:
            # don't do things inside of "except Empty", because when we say
            # we timed out or can't connect and raise, Python 3 tells
            # people the real error is queue.Empty which it isn't.
            pass
        if use_overflow and self._overflow >= self._max_overflow:
            if not wait:
                return self._do_acquire_conn()
            else:
                raise TimeoutError(
                    'Pool limit of size {} overflow {} reached, '
                    'connection timed out, timeout {}'.format(self.size, self._overflow, self._timeout),
                    )

        if self._inc_overflow():
            try:
                return self._create_conn()
            except Exception:
                self._dec_overflow()
        else:
            return self._do_acquire_conn()

    def ping_conn(self, conn):
        try:
            r = conn.ping()
            if not isinstance(r, bool):
                return True
            return r
        except Exception:
            return False

    def clear_conns(self):
        while True:
            try:
                conn = self._pool.get(False)
                conn.do_real_close()
            except Empty:
                break

        self._overflow = 0 - self.size

    @log_pool('destroy conn: {conn}')
    def destroy_conn(self, conn):
        try:
            if not conn.is_closed:
                conn.do_real_close()
        finally:
            self._dec_overflow()

    @log_pool('release conn: {conn}')
    def release_conn(self, conn: 'ConnProxy'):
        if conn.is_released:
            logger.warn('%s attempted to double release!', conn)
            return
        try:
            self._pool.put(conn, False)
            conn.is_released = True
        except Full:
            self.destroy_conn(conn)
