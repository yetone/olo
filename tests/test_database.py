# coding: utf-8

from mock import Mock

from olo import DataBase
from olo.errors import DataBaseError
from .base import TestCase, db as store, beansdb
from .utils import AE


class TestDataBase(TestCase):
    def test_db_get(self):
        db = DataBase(store)
        with self.assertRaises(DataBaseError):
            db.db_get('a')
        db = DataBase(store, beansdb=beansdb)
        db.db_get('a')

    def test_db_set(self):
        db = DataBase(store)
        with self.assertRaises(DataBaseError):
            db.db_set('a', 'b')
        db = DataBase(store, beansdb=beansdb)
        db.db_set('a', 'b')
        self.assertEqual(db.db_get('a'), 'b')

    def test_db_get_multi(self):
        db = DataBase(store)
        with self.assertRaises(DataBaseError):
            db.db_get_multi(['a', 'b'])
        db = DataBase(store, beansdb=beansdb)
        db.db_get_multi(['a', 'b'])

    def test_db_set_multi(self):
        db = DataBase(store)
        with self.assertRaises(DataBaseError):
            db.db_set_multi({'a': 1, 'b': 2})
        db = DataBase(store, beansdb=beansdb)
        db.db_set_multi({'a': 1, 'b': 2})
        self.assertEqual(db.db_get('a'), 1)
        self.assertEqual(db.db_get('b'), 2)

    def test_db_delete_multi(self):
        db = DataBase(store, beansdb=beansdb)
        db.db_delete_multi(['a', 'b'])
        self.assertIsNone(db.db_get('a'))
        self.assertIsNone(db.db_get('b'))

    def test_do_beansdb_commands(self):
        db = DataBase(store)
        with db.transaction():
            with self.assertRaises(DataBaseError):
                db.db_set('a', 'xixi')
        db = DataBase(store, beansdb=beansdb)
        oa = db.db_get('a')
        ob = db.db_get('b')
        try:
            with db.transaction():
                db.db_set('a', 'xixi')
                db.db_set('b', 'haha')
                raise AE
        except AE:
            pass
        self.assertEqual(db.db_get('a'), oa)
        self.assertEqual(db.db_get('b'), ob)
        with db.transaction():
            db.db_set('a', 'xixi')
            db.db_set('b', 'haha')
        self.assertEqual(db.db_get('a'), 'xixi')
        self.assertEqual(db.db_get('b'), 'haha')
        db._local.append_beansdb_commands(('error', (), {}))
        db._do_beansdb_commands()

    def test_cancel_transaction(self):
        db = DataBase(store)
        self.assertFalse(db.in_transaction())
        r = db.cancel_transaction()
        self.assertFalse(r)
        with db.transaction():
            self.assertTrue(db.in_transaction())
            id = db.execute(
                'insert into foo(name, age, age_str, `key`) values(%s, %s, %s, %s)',
                ('foo', 1, '1', '1')
            )
            r = db.cancel_transaction()
            self.assertTrue(r)
        rv = db.execute(
            'select * from foo where id = %s',
            id
        )
        self.assertEqual(rv, ())

    def test_add_commit_handler(self):
        db = DataBase(store)
        old_report = db.report
        db.report = Mock()
        handler = Mock()
        db.add_commit_handler(handler)
        db.commit()
        self.assertEqual(db.report.call_count, 0)
        self.assertTrue(handler.called)
        self.assertEqual(handler.call_count, 1)
        handler = Mock()
        handler.side_effect = AE()
        db.add_commit_handler(handler)
        db.commit()
        self.assertEqual(db.report.call_count, 1)
        db.report = old_report

    def test_add_lazy_func(self):
        db = DataBase(store)
        old_report = db.report
        db.report = Mock()
        func = Mock()
        db.add_lazy_func(func)
        db.commit()
        self.assertEqual(db.report.call_count, 0)
        self.assertTrue(func.called)
        self.assertEqual(func.call_count, 1)
        func = Mock()
        func.side_effect = AE()
        db.add_lazy_func(func)
        db.commit()
        self.assertEqual(db.report.call_count, 1)
        db.report = old_report
