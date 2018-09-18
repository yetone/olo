from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor
from olo.store import ThreadSafeStore

from .base import TestCase, Dummy, db as _store


store = ThreadSafeStore(_store, queue_size=64)


class TestThreadSafeStore(TestCase):

    @staticmethod
    def create_dummies():
        Dummy._get_db()._store = store

        def create(idx):
            Dummy.create(name='foo%s' % idx, age=idx)
        with ThreadPoolExecutor(max_workers=8) as exe:
            list(exe.map(create, xrange(1, 9)))

        Dummy._get_db()._store = _store

    def _test_execute(self):
        self.create_dummies()

        def get(age):
            rv = store.execute('select `name` from dummy where age = %s', age)
            if rv:
                return rv[0][0]

        with ThreadPoolExecutor(max_workers=8) as exe:
            res = list(exe.map(get, xrange(1, 9)))

        self.assertEqual(res, ['foo%s' % i for i in xrange(1, 9)])

    def _test_commit(self):
        store.execute(
            'insert into dummy(`name`, age, tags, payload,'
            ' created_at, created_date, updated_at) '
            'values(%s, %s, %s, %s, %s, %s, %s)',
            ('foo', 1, '', '', datetime.now(), date.today(), datetime.now())
        )
        store.commit()
        rv = store.execute('select `name` from dummy where age = %s', 1)
        self.assertEqual(rv[0][0], 'foo')

    def _test_rollback(self):
        store.execute(
            'insert into dummy(`name`, age, tags, payload,'
            ' created_at, created_date, updated_at) '
            'values(%s, %s, %s, %s, %s, %s, %s)',
            ('foo', 1, '', '', datetime.now(), date.today(), datetime.now())
        )
        store.rollback()
        rv = store.execute('select `name` from dummy where age = %s', 1)
        self.assertEqual(rv, tuple())
