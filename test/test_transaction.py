import time
from mock import Mock
from .base import db, TestCase, Foo
from .utils import AE


attrs = dict(
    name='foo',
    tags=['a', 'b', 'c'],
    payload={
        'abc': [1, 2, 3],
        'def': [4, 5, 6]
    }
)


class TestTransaction(TestCase):
    def test_create(self):
        with db.transaction():
            foo = Foo.create()
        self.assertEqual(foo.id, Foo.cache.get(foo.id).id)
        self.assertEqual(foo.id, 1)
        try:
            with db.transaction():
                foo = Foo.create(name='foo', age=1, key='1')
                foo = Foo.create(name='foo', age=2, key='1')
        except Exception:
            pass
        count = Foo.count_by()
        self.assertEqual(count, 1)

    def test_update(self):
        Foo.create(name='foo', age=1, key='1')
        foo = Foo.create(name='foo', age=2, key='2', prop1=[1, 2, 3])
        foo.key = '1'
        try:
            with db.transaction():
                foo.save()
        except Exception:
            pass
        foo = Foo.query.filter(id=foo.id).first()
        self.assertEqual(foo.key, '2')
        self.assertEqual(foo.prop1, [1, 2, 3])
        try:
            with db.transaction():
                foo.prop1 = [4]
                foo.save()
                raise AE
        except AE:
            pass
        self.assertEqual(foo.prop1, [1, 2, 3])
        foo = Foo.query.filter(id=foo.id).first()
        self.assertEqual(foo.prop1, [1, 2, 3])
        try:
            with db.transaction():
                foo.update(name='xixi')
                raise AE
        except AE:
            pass
        foo = Foo.query.filter(id=foo.id).first()
        self.assertEqual(foo.name, 'foo')
        foo = Foo.cache.get(foo.id)
        self.assertEqual(foo.name, 'foo')
        with db.transaction():
            foo.update(name='cool')
            self.assertEqual(foo.name, 'cool')

    def test_delete(self):
        foo = Foo.create()
        try:
            with db.transaction():
                foo.delete()
                raise AE
        except AE:
            pass
        foo = Foo.query.filter(id=foo.id).first()
        self.assertTrue(foo is not None)

    def _test_nested(self):
        with db.transaction():
            foo = Foo.create(name='foo', age=1, key='1', prop1=[1])
            with db.transaction():
                Foo.create(name='foo', age=2, key='2')
                foo.prop1 = [2, 3]
                try:
                    with db.transaction():
                        foo.prop1 = [4, 5, 6]
                        Foo.create(name='foo', age=3, key='1')
                except Exception:
                    pass
        count = Foo.count_by()
        self.assertEqual(count, 2)
        foo = Foo.get_by(key='1')
        self.assertEqual(foo.id, 1)
        self.assertEqual(foo.prop1, [2, 3])

        try:
            with db.transaction():
                foo = Foo.create(name='foo', age=3, key='3', prop1=[1])
                with db.transaction():
                    Foo.create(name='foo', age=4, key='4')
                    try:
                        with db.transaction():
                            foo.prop1 = [4, 5, 6]
                            Foo.create(name='foo', age=3, key='3')
                    except Exception:
                        pass
                raise AE
        except AE:
            pass
        foo = Foo.get_by(key='3')
        self.assertTrue(foo is None)

    def _test_transaction_table(self):
        db = Foo._get_db()
        try:
            with db.transaction():
                cur_id0 = id(db._transaction_store.cursor)
                foo = Foo.create(name='foo', age=3, key='3', prop1=[1])
                with db.transaction():
                    cur_id1 = id(db._transaction_store.cursor)
                    Foo.create(name='foo', age=4, key='4')
                    db._transaction_store.expire_time = time.time()
                    db._transaction_store.expire_ts = 0
                    try:
                        with db.transaction():
                            foo.prop1 = [4, 5, 6]
                            cur_id2 = id(db._transaction_store.cursor)
                            Foo.create(name='foo', age=3, key='3')
                    except Exception:
                        pass
                raise AE
        except AE:
            pass
        foo = Foo.get_by(key='3')
        self.assertTrue(foo is None)
        self.assertEqual(cur_id0, cur_id1)
        self.assertEqual(cur_id0, cur_id2)
        try:
            with db.transaction():
                cur_id3 = id(db._transaction_store.cursor)
                foo = Foo.create(name='foo', age=6, key='6', prop1=[1])
                with db.transaction():
                    cur_id4 = id(db._transaction_store.cursor)
                    Foo.create(name='foo', age=7, key='7')
                    try:
                        with db.transaction():
                            foo.prop1 = [4, 5, 6]
                            cur_id5 = id(db._transaction_store.cursor)
                            Foo.create(name='foo', age=8, key='8')
                    except Exception:
                        pass
        except Exception:
            pass
        self.assertEqual(cur_id3, cur_id4)
        self.assertEqual(cur_id3, cur_id5)
        self.assertNotEqual(cur_id0, cur_id3)
        self.assertEqual(foo.key, '6')
        foo = Foo.get(foo.id)
        self.assertEqual(foo.age, 6)
        try:
            with db.transaction():
                cur_id6 = id(db._transaction_store.cursor)
                foo = Foo.create(name='foo', age=9, key='9', prop1=[1])
                with db.transaction():
                    cur_id7 = id(db._transaction_store.cursor)
                    Foo.create(name='foo', age=10, key='10')
                    try:
                        with db.transaction():
                            foo.prop1 = [4, 5, 6]
                            cur_id8 = id(db._transaction_store.cursor)
                            Foo.create(name='foo', age=11, key='11')
                    except Exception:
                        pass
                raise AE
        except AE:
            pass
        self.assertEqual(cur_id6, cur_id7)
        self.assertEqual(cur_id6, cur_id8)
        self.assertNotEqual(cur_id3, cur_id6)

    def test_cancel(self):
        foo = Foo.create(name='foo', age=1)

        def func1(foo):
            with db.transaction():
                foo.update(name='xixi', age=2)
                return

        func1(foo)
        foo = Foo.cache.get(foo.id)
        self.assertEqual(foo.name, 'xixi')
        self.assertEqual(foo.age, 2)

        def func2(foo):
            with db.transaction():
                foo.update(name='hehe', age=3)
                db.cancel_transaction()
                return

        func2(foo)
        foo = Foo.cache.get(foo.id)
        self.assertEqual(foo.name, 'xixi')
        self.assertEqual(foo.age, 2)

        def func3(foo):
            with db.transaction():
                foo.update(name='hehe', age=3)
                db.cancel_transaction()
                with db.transaction():
                    foo.update(name='wowo', age=4)
                    db.cancel_transaction()
                    return

        func3(foo)
        foo = Foo.cache.get(foo.id)
        self.assertEqual(foo.name, 'xixi')
        self.assertEqual(foo.age, 2)

    def test_commit(self):
        foo = Foo.create(name='foo', age=1)

        with db.transaction() as tran:
            foo.update(name='xixi', age=2)
            tran.commit(True)

        foo = Foo.get(foo.id)
        self.assertEqual(foo.name, 'xixi')

    def test_rollback(self):
        foo = Foo.create(name='foo', age=1)

        with db.transaction() as tran:
            foo.update(name='xixi', age=2)
            tran.rollback(True)

        foo = Foo.get(foo.id)
        self.assertEqual(foo.name, 'foo')

    def test_exit(self):
        foo = Foo.create(name='foo', age=1)

        with self.assertRaises(AE):
            with db.transaction() as tran:
                foo.update(name='xixi', age=2)
                tran.commit = Mock()
                tran.commit.side_effect = AE()

        foo = Foo.get(foo.id)
        self.assertEqual(foo.name, 'foo')

    def test_decorator(self):

        @db.transaction()
        def func():
            return Foo.create()

        foo = func()

        self.assertEqual(foo.id, Foo.cache.get(foo.id).id)
        self.assertEqual(foo.id, 1)

        try:

            @db.transaction()
            def func1():
                foo = Foo.create(name='foo', age=1, key='1')
                foo = Foo.create(name='foo', age=2, key='1')

            func1()

        except Exception:
            pass

        count = Foo.count_by()
        self.assertEqual(count, 1)
