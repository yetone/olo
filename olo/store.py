import time
import atexit
import threading

from olo.compat import Queue, Empty, iteritems


def is_under_thread():
    return threading.current_thread().name != 'MainThread'


class ThreadSafeStore(object):
    modified_cursor_q = Queue()

    def __init__(self, store, timeout=60 * 60, queue_size=16):
        self.lock = threading.RLock()
        self.store = store
        self.timeout = timeout
        self.cursors = {}
        self.queue_size = queue_size
        atexit.register(self.clear_cursors)

    def get_cursor(self, table='*'):
        if table:
            luz_cur = self.store.get_cursor(table=table)
        else:
            luz_cur = self.store.get_cursor()
        if not is_under_thread():
            return luz_cur
        with self.lock:
            q = self.cursors.get(table)
            if not q:
                q = self.cursors[table] = Queue(maxsize=self.queue_size)
        try:
            cur = q.get_nowait()
            if cur.is_closed or cur.expire_time <= time.time():
                self.close_cursor(cur)
                return self.get_cursor(table=table)
        except Empty:
            cur = self.new_cursor(luz_cur)
        return cur

    def clear_cursors(self):
        with self.lock:
            for k, v in iteritems(self.cursors):
                while not v.empty():
                    try:
                        cur = v.get_nowait()
                        self.close_cursor(cur)
                    except Empty:
                        pass
            self.cursors.clear()

    def release_cursor(self, cur, table='*'):
        if not is_under_thread():
            return
        with self.lock:
            q = self.cursors.get(table)
            if not q:
                q = self.cursors[table] = Queue(maxsize=64)
        if q.full() or cur.expire_time <= time.time():
            self.close_cursor(cur)
            return
        q.put_nowait(cur)

    def new_cursor(self, luz_cur):
        farm = luz_cur.farm
        cur = farm.connect(**farm.dbcnf)
        cur.expire_time = time.time()
        cur.is_closed = False
        return cur

    def close_cursor(self, cur):
        cur.connection.close()
        cur.is_closed = True

    def execute(self, sql, *args, **kwargs):
        if not is_under_thread():
            self.clear_cursors()
            return self.store.execute(sql, *args, **kwargs)

        tables = None
        cmd = None
        try:
            cmd, tables = self.store.parse_execute_sql(sql)
        except Exception:
            pass
        cur = self.get_cursor(table=tables[0])
        res = cur.execute(sql, *args, **kwargs)
        if cmd == 'select':
            res = cur.fetchall()
            self.release_cursor(cur)
            return res
        self.modified_cursor_q.put_nowait(cur)
        if (
            not kwargs.get('executemany') and
            cmd == 'insert' and
            cur.lastrowid
        ):
            res = cur.lastrowid
        return res

    def commit(self):
        if not is_under_thread():
            return self.store.commit()
        while not self.modified_cursor_q.empty():
            try:
                cur = self.modified_cursor_q.get_nowait()
                if cur.is_closed:
                    continue
                cur.execute('commit')
                self.release_cursor(cur)
            except Empty:
                pass

    def rollback(self):
        if not is_under_thread():
            return self.store.rollback()
        while not self.modified_cursor_q.empty():
            try:
                cur = self.modified_cursor_q.get_nowait()
                if cur.is_closed:
                    continue
                cur.execute('rollback')
                self.release_cursor(cur)
            except Empty:
                pass
