# coding: utf-8

import time
from concurrent.futures import ThreadPoolExecutor

from test.base import TestCase, db, Foo


class TestPool(TestCase):
    def test_acquire(self):
        self.assertEquals(db.pool.active_size, 0)
        self.assertEquals(db.pool.idle_size, 0)
        conn = db.pool.acquire_conn()
        self.assertEquals(db.pool.active_size, 1)
        self.assertEquals(db.pool.idle_size, 0)
        db.pool.release_conn(conn)
        self.assertEquals(db.pool.active_size, 0)
        self.assertEquals(db.pool.idle_size, 1)
        conn1 = db.pool.acquire_conn()
        db.pool.release_conn(conn1)
        self.assertEquals(conn.id, conn1.id)
        self.assertEquals(db.pool.active_size, 0)
        self.assertEquals(db.pool.idle_size, 1)

    def test_acquire_expired_conn(self):
        self.assertEquals(db.pool.active_size, 0)
        self.assertEquals(db.pool.idle_size, 0)
        conn = db.pool.acquire_conn()
        db.pool.release_conn(conn)
        self.assertEquals(db.pool.active_size, 0)
        self.assertEquals(db.pool.idle_size, 1)
        conn.expire_time = time.time()
        conn1 = db.pool.acquire_conn()
        db.pool.release_conn(conn1)
        self.assertNotEquals(conn.id, conn1.id)
        self.assertEquals(db.pool.active_size, 0)
        self.assertEquals(db.pool.idle_size, 1)

    def test_release(self):
        Foo.create(name='foo', age=12, key='a')
        with ThreadPoolExecutor(max_workers=10) as exe:
            list(exe.map(Foo._get, range(db.pool.max_idle_size * 2)))
        self.assertEquals(db.pool.active_size, 0)
        self.assertEquals(db.pool.idle_size, db.pool.max_idle_size)
