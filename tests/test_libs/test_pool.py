# coding: utf-8

import time
from concurrent.futures import ThreadPoolExecutor

from tests.base import TestCase, db, Foo


class TestPool(TestCase):
    def test_acquire(self):
        self.assertEqual(db.pool.idle_size, 0)
        conn = db.pool.acquire_conn()
        self.assertEqual(db.pool.idle_size, 0)
        db.pool.release_conn(conn)
        self.assertEqual(db.pool.idle_size, 1)
        conn1 = db.pool.acquire_conn()
        db.pool.release_conn(conn1)
        self.assertEqual(conn.id, conn1.id)
        self.assertEqual(db.pool.idle_size, 1)

    def test_acquire_expired_conn(self):
        self.assertEqual(db.pool.idle_size, 0)
        conn = db.pool.acquire_conn()
        db.pool.release_conn(conn)
        self.assertEqual(db.pool.idle_size, 1)
        conn.expire_time = time.time()
        conn1 = db.pool.acquire_conn()
        db.pool.release_conn(conn1)
        self.assertNotEqual(conn.id, conn1.id)
        self.assertEqual(db.pool.idle_size, 1)

    def test_release(self):
        Foo.create(name='foo', age=12, key='a')
        with ThreadPoolExecutor(max_workers=10) as exe:
            list(exe.map(Foo._get, range(10)))
        self.assertEqual(db.pool.idle_size, db.pool.size)
