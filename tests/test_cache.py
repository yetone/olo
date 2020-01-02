# coding: utf-8
from olo.logger import logger
from .base import db, TestCase, Dummy, Foo, Bar, Lala
from .utils import (
    auto_use_cache_ctx, patched_execute, no_cache_client,
    no_pk, AE
)
from olo.cache import create_cache
from olo.utils import missing
from olo.errors import CacheError


attrs = dict(
    name='foo',
    tags=['a', 'b', 'c'],
    password='password',
    payload={
        'abc': ['1', 2, 3],
        'def': [4, '5', 6]
    }
)


class TestCache(TestCase):
    def test_get(self):
        dummy = Dummy.create(**attrs)
        with patched_execute as execute:
            _dummy = Dummy.cache.get(dummy.id)
            self.assertEqual(dummy.id, _dummy.id)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            _dummy = Dummy.cache.get(dummy.id)
            self.assertEqual(dummy.id, _dummy.id)
            self.assertFalse(execute.called)
        _dummy.update(age=666)
        with patched_execute as execute:
            _dummy = Dummy.cache.get(dummy.id)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            _dummy = Dummy.cache.get(dummy.id)
            self.assertFalse(execute.called)
            self.assertEqual(dummy.id, _dummy.id)
            self.assertEqual(_dummy.age, 666)
        with patched_execute as execute:
            Dummy.get(dummy.id)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            _dummy = Dummy.cache.get(233)
            self.assertIsNone(_dummy)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            _dummy = Dummy.cache.get(233)
            self.assertIsNone(_dummy)
            self.assertFalse(execute.called)
        Dummy.create(id=233, **attrs)
        with patched_execute as execute:
            _dummy = Dummy.cache.get(233)
            self.assertIsNotNone(_dummy)
            self.assertEqual(_dummy.id, 233)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            _dummy = Dummy.cache.get(233)
            self.assertIsNotNone(_dummy)
            self.assertEqual(_dummy.id, 233)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            with auto_use_cache_ctx(Dummy):
                _dummy = Dummy.cache.get(233)
                self.assertIsNotNone(_dummy)
                self.assertEqual(_dummy.id, 233)
                self.assertFalse(execute.called)
        with no_cache_client(Dummy):
            with patched_execute as execute:
                _dummy = Dummy.cache.get(233)
                self.assertIsNotNone(_dummy)
                self.assertEqual(_dummy.id, 233)
                self.assertTrue(execute.called)
        with patched_execute as execute:
            foo = Foo.cache.get(name='170331', age=1)
            self.assertIsNone(foo)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            foo = Foo.cache.get(name='170331', age=1)
            self.assertIsNone(foo)
            self.assertFalse(execute.called)
        Foo.create(name='170331', age=1)
        with patched_execute as execute:
            foo = Foo.cache.get(name='170331', age=1)
            self.assertIsNotNone(foo)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            foo = Foo.cache.get(name='170331', age=1)
            self.assertIsNotNone(foo)
            self.assertFalse(execute.called)

    def test_update(self):
        dummy = Dummy.create(**attrs)
        dummy.name = 'xixi'
        dummy.save()
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.name, dummy.name)
        dummy.update(name='hehe')
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.name, dummy.name)
        _dummy.update(name='wow')
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.name, 'wow')

    def test_delete(self):
        dummy = Dummy.create(**attrs)
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.id, dummy.id)
        dummy.delete()
        _dummy = Dummy.cache.get(dummy.id)
        self.assertTrue(_dummy is None)
        dummy = Dummy.create(**attrs)
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.id, dummy.id)
        _dummy.delete()
        _dummy = Dummy.cache.get(dummy.id)
        self.assertTrue(_dummy is None)
        foo = Foo.create(name='foo', age=1)
        _foo = Foo.cache.get_by(name='foo', age='1')
        self.assertEqual(foo.id, _foo.id)
        _foo.delete()
        _foo = Foo.cache.get_by(name='foo', age='1')
        self.assertTrue(_foo is None)
        dummy = Dummy.create(**attrs)
        _dummy = Dummy.cache.get(dummy.id)
        with no_cache_client(Dummy):
            dummy.delete()
        _dummy = Dummy.cache.get(dummy.id)
        self.assertTrue(_dummy is not None)

    def test_transaction(self):
        dummy = Dummy.create(**attrs)
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.name, dummy.name)
        with db.transaction():
            dummy.update(name='lala')
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.name, 'lala')
        try:
            with db.transaction():
                dummy.update(name='hehe')
                raise AE
        except AE:
            pass
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.name, 'lala')
        try:
            with db.transaction():
                dummy.update(name='hehe')
                _dummy = Dummy.cache.get(dummy.id)
                self.assertEqual(_dummy.name, 'hehe')
                dummy.update(name='xixi')
                _dummy = Dummy.cache.get(dummy.id)
                self.assertEqual(_dummy.name, 'xixi')
                raise AE
        except AE:
            pass
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(_dummy.name, 'lala')
        with db.transaction():
            dummy.delete()
        _dummy = Dummy.cache.get(dummy.id)
        self.assertTrue(_dummy is None)

        foo = Foo.create(name='lala', age=1)
        try:
            with db.transaction():
                foo.update(name='xixi')
                foo = Foo.cache.get_by(age=foo.age)
                self.assertEqual(foo.name, 'xixi')
                raise AE
        except AE:
            pass
        foo = Foo.cache.get_by(age=foo.age)
        self.assertEqual(foo.name, 'lala')

    def test_gets(self):
        Foo.create(name='abc', age=1)
        Foo.create(name='qwe', age=2)
        Foo.create(name='xxx', age=1)
        Foo.create(name='yyy', age=1)
        idents = [
            {'name': 'xxx', 'age': 1},
            {'name': 'abc', 'age': 1},
        ]
        with patched_execute as execute:
            foos = Foo.cache.gets(idents)
            self.assertTrue(execute.called)
            self.assertEqual(len(foos), 2)
            self.assertEqual(foos[0].name, 'xxx')
            self.assertEqual(foos[1].name, 'abc')
        with patched_execute as execute:
            foos = Foo.cache.gets(idents)
            self.assertFalse(execute.called)
            self.assertEqual(len(foos), 2)
            self.assertEqual(foos[0].name, 'xxx')
            self.assertEqual(foos[1].name, 'abc')
        with patched_execute as execute:
            idents.extend([
                {'name': 'qwe', 'age': 1},
                {'name': 'yyy', 'age': 1}
            ])
            foos = Foo.cache.gets(idents, filter_none=False)
            self.assertTrue(execute.called)
            self.assertEqual(len(foos), 4)
            self.assertIsNone(foos[2])
        with patched_execute as execute:
            foos = Foo.cache.gets(idents, filter_none=False)
            self.assertFalse(execute.called)
            self.assertEqual(len(foos), 4)
            self.assertIsNone(foos[2])
        with patched_execute as execute:
            foos = Foo.cache.gets(idents, filter_none=True)
            self.assertFalse(execute.called)
            self.assertEqual(len(foos), 3)
        with patched_execute as execute:
            with auto_use_cache_ctx(Foo):
                foos = Foo.cache.gets(idents, filter_none=False)
                self.assertFalse(execute.called)
                self.assertEqual(len(foos), 4)
                self.assertIsNone(foos[2])
        with patched_execute as execute:
            with no_cache_client(Foo):
                foos = Foo.cache.gets(idents, filter_none=False)
                self.assertTrue(execute.called)
                self.assertEqual(len(foos), 4)
                self.assertIsNone(foos[2])
        with patched_execute as execute:
            with no_pk(Foo):
                foos = Foo.cache.gets(idents, filter_none=False)
                self.assertFalse(execute.called)
                self.assertEqual(len(foos), 4)
                self.assertIsNone(foos[2])
        with self.assertRaises(CacheError):
            Foo.cache.gets([{'age_str': 'b'}])

    def test_get_by(self):
        Foo.create(name='abc', age=1)
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='abc', age=1)
            self.assertTrue(execute.called)
            self.assertEqual(foo.name, 'abc')
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='abc', age=1)
            self.assertFalse(execute.called)
            self.assertEqual(foo.name, 'abc')
        with patched_execute as execute:
            foo = Foo.cache.get_by(key=foo.key)
            self.assertTrue(execute.called)
            self.assertEqual(foo.name, 'abc')
        with patched_execute as execute:
            foo = Foo.cache.get_by(key=foo.key)
            self.assertFalse(execute.called)
            self.assertEqual(foo.name, 'abc')
        foo.name = 'qwe'
        foo.name = 'hehe'
        foo.save()
        with patched_execute as execute:
            Foo.cache.get_by(key=foo.key)
            self.assertTrue(execute.called)
        foo = Foo.cache.get_by(key=foo.key)
        self.assertEqual(foo.name, 'hehe')
        with patched_execute as execute:
            foo = Foo.cache.get_by(key=foo.key)
            self.assertFalse(execute.called)
            self.assertEqual(foo.name, 'hehe')
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='abc', age=1)
            self.assertTrue(execute.called)
            self.assertIsNone(foo)
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='missing', age=1)
            self.assertTrue(execute.called)
            self.assertIsNone(foo)
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='missing', age=1)
            self.assertFalse(execute.called)
            self.assertIsNone(foo)
        Foo.create(name='missing', age=1)
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='missing', age=1)
            self.assertTrue(execute.called)
            self.assertIsNotNone(foo)
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='missing', age=1)
            self.assertFalse(execute.called)
            self.assertIsNotNone(foo)
            self.assertEqual(foo.name, 'missing')
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='missing', age=1)
            self.assertFalse(execute.called)
            self.assertIsNotNone(foo)
            self.assertEqual(foo.name, 'missing')
        with patched_execute as execute:
            foo = Foo.cache.get_by(age=1)
            self.assertIsNotNone(foo)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            foo = Foo.cache.get_by(key='aaa')
            self.assertIsNone(foo)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            foo = Foo.cache.get_by(key='aaa')
            self.assertIsNone(foo)
            self.assertFalse(execute.called)
        Foo.create(key='aaa')
        with patched_execute as execute:
            foo = Foo.cache.get_by(key='aaa')
            self.assertIsNotNone(foo)
            self.assertTrue(execute.called)
        bar = Bar.create(name='a', xixi='a', age=1)
        with patched_execute as execute:
            _bar = Bar.cache.get_by(xixi='b', age=1)
            self.assertIsNone(_bar)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            _bar = Bar.cache.get_by(xixi='b', age=1)
            self.assertIsNone(_bar)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            _bar = Bar.cache.get_by(xixi='a', age=1)
            self.assertEqual(bar.name, _bar.name)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            _bar = Bar.cache.get_by(xixi='a', age=1)
            self.assertEqual(bar.name, _bar.name)
            self.assertFalse(execute.called)
        bar = Bar.create(name='ab', xixi='ab', age=1, word='1')
        with patched_execute as execute:
            _bar = Bar.cache.get_by(xixi='ab', age=1, word='1')
            self.assertEqual(bar.word, _bar.word)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            _bar = Bar.cache.get_by(xixi='ab', age=1, word='1')
            self.assertEqual(bar.word, _bar.word)
            self.assertTrue(execute.called)

    def test_uk_update(self):
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='170331', age=1)
            self.assertIsNone(foo)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='170331', age=1)
            self.assertIsNone(foo)
            self.assertFalse(execute.called)
        foo = Foo.create(name='abc', age=1)
        foo.update(name='170331')
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='170331', age=1)
            self.assertIsNotNone(foo)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            foo = Foo.cache.get_by(name='170331', age=1)
            self.assertIsNotNone(foo)
            self.assertFalse(execute.called)

    def test_gets_by(self):
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1)
            self.assertEqual(bars, [])
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1)
            self.assertEqual(bars, [])
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=10)
            self.assertEqual(bars, [])
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(bars, [])
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(limit=10)
            self.assertEqual(bars, [])
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(limit=11)
            self.assertEqual(bars, [])
            self.assertFalse(execute.called)
        bar = Bar.create(name='a', xixi='a', age=1)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
            self.assertEqual(execute.call_count, 2)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(limit=10)
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
        bar.update(name='a+')
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
            self.assertEqual(execute.call_count, 2)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        bar.update(name='a')
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
            self.assertEqual(execute.call_count, 2)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        bar.update(word='1')
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
            self.assertEqual(execute.call_count, 1)
            self.assertEqual(bars[0].word, bar.word)
        bar.update(word='2')
        Bar.cache.get(bar.name)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
            self.assertEqual(bars[0].word, bar.word)
        bar.update(xixi='b')
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='a', age=1, limit=11)
            self.assertEqual(len(bars), 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, limit=11)
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        bar.update(word='a')
        bar = Bar.create(name='b', xixi='b', age=1, word='b')
        bar = Bar.create(name='c', xixi='b', age=1, word='c')
        bar = Bar.create(name='d', xixi='b', age=1, word='d')
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, limit=11)
            self.assertEqual(len(bars), 4)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, limit=11)
            self.assertEqual(len(bars), 4)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1)
            self.assertEqual(len(bars), 4)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=1)
            self.assertEqual(len(bars), 3)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, limit=2)
            self.assertEqual(len(bars), 2)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2)
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1,
                                     limit=Bar.cache.MAX_COUNT + 1)
            self.assertEqual(len(bars), 4)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1,
                                     limit=Bar.cache.MAX_COUNT + 1)
            self.assertEqual(len(bars), 4)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by='xixi')
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by='xixi')
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by='age')
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by='age')
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1,
                                     limit=3, order_by='-name')
            self.assertEqual(len(bars), 3)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1,
                                     limit=3, order_by='-name')
            self.assertEqual(len(bars), 3)
            self.assertEqual(['d', 'c', 'b'], list(map(lambda x: x.name, bars)))
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1,
                                     limit=3, order_by='name')
            self.assertEqual(len(bars), 3)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1,
                                     limit=3, order_by='name')
            self.assertEqual(len(bars), 3)
            self.assertEqual(['a', 'b', 'c'], list(map(lambda x: x.name, bars)))
            self.assertFalse(execute.called)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by=('-age', 'word'))
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by=('-age', 'word'))
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)

        with patched_execute as execute:
            with auto_use_cache_ctx(Bar):
                bars = Bar.gets_by(xixi='b', age=1, start=3,
                                   limit=2, order_by=('-age', 'word'))
                self.assertEqual(len(bars), 1)
                self.assertFalse(execute.called)

        _bar = bars[0]
        _bar.update(xixi='c')
        logger.debug('[BAR]: %s', _bar)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=2,
                                     limit=2, order_by=('-age', 'word'))
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)

        _bar.update(xixi='e')
        logger.debug('[BAR]: %s', _bar)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=2,
                                     order_by=('-age', 'word'))
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)

        _bar.update(xixi='b')
        logger.debug('[BAR]: %s', _bar)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by=('word', 'age'))
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by=('word', 'age'))
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)

        _bar.update(xixi='e')
        logger.debug('[BAR]: %s', _bar)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=2,
                                     order_by=('-age', 'word'))
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)

        Bar.create(name='e', xixi='b', age=1, word='e')
        Bar.create(name='f', xixi='b', age=1, word='f')

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by=('word', 'age'))
            self.assertEqual(len(bars), 2)
            self.assertTrue(execute.called)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2, order_by=['word', 'age'])
            self.assertEqual(len(bars), 2)
            self.assertFalse(execute.called)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(name='e')
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)

        Foo.create(name='1', age=1)
        Foo.create(name='2', age=1)
        Foo.create(name='3', age=2)

        with no_pk(Foo):
            Foo.cache.gets_by(age=1, limit=3)

        foos = Foo.cache.gets_by(age=3, limit=3)
        self.assertEqual(foos, [])

        # test unique key
        foos = Foo.cache.gets_by(name='1', age=1)
        self.assertEqual(len(foos), 1)
        foos = Foo.cache.gets_by(name='100', age=1)
        self.assertEqual(foos, [])

    def test_gets_by_with_order_by(self):
        b0 = Bar.create(name='e', xixi='b', age=1)
        b1 = Bar.create(name='f', xixi='a', age=1)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(age=1, order_by=('xixi', 'age'))
            self.assertEqual(bars, [b1, b0])
            self.assertTrue(execute.called)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(age=1, order_by=['xixi', 'age'])
            self.assertEqual(bars, [b1, b0])
            self.assertFalse(execute.called)

        b1.update(xixi='c')

        with patched_execute as execute:
            bars = Bar.cache.gets_by(age=1, order_by=['xixi', 'age'])
            self.assertEqual(bars, [b0, b1])
            self.assertTrue(execute.called)

        with patched_execute as execute:
            bars = Bar.cache.gets_by(age=1, order_by=['xixi', 'age'])
            self.assertEqual(bars, [b0, b1])
            self.assertFalse(execute.called)

    def test_gets_by_missing_value(self):
        Bar.create(name='b', xixi='b', age=1)
        Bar.create(name='c', xixi='b', age=1)
        Bar.create(name='d', xixi='b', age=1)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=missing)
            self.assertEqual(len(bars), 3)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=missing)
            self.assertEqual(len(bars), 3)
            self.assertFalse(execute.called)

    def test_gets_by_over_limit(self):
        max_count = Bar.cache.MAX_COUNT
        Bar.create(name='b', xixi='b', age=1)
        Bar.create(name='c', xixi='b', age=1)
        Bar.create(name='d', xixi='b', age=1)
        Bar.cache.MAX_COUNT = 2
        try:
            with patched_execute as execute:
                bars = Bar.cache.gets_by(xixi='b', age=1)
                self.assertEqual(len(bars), 3)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                bars = Bar.cache.gets_by(xixi='b', age=1)
                self.assertEqual(len(bars), 3)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                bars = Bar.cache.gets_by(xixi='b', age=1, limit=2)
                self.assertEqual(len(bars), 2)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                bars = Bar.cache.gets_by(xixi='b', age=1, limit=2)
                self.assertEqual(len(bars), 2)
                self.assertFalse(execute.called)
            with patched_execute as execute:
                bars = Bar.cache.gets_by(xixi='b', age=1, start=1)
                self.assertEqual(len(bars), 3)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                bars = Bar.cache.gets_by(xixi='b', age=1, start=1)
                self.assertEqual(len(bars), 3)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                bars = Bar.cache.gets_by(xixi='b', age=1, start=3)
                self.assertEqual(len(bars), 3)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                bars = Bar.cache.gets_by(xixi='b', age=1, start=3)
                self.assertEqual(len(bars), 3)
                self.assertTrue(execute.called)
        finally:
            Bar.cache.MAX_COUNT = max_count

    def test_count_by(self):
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='a', age=1)
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='a', age=1)
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='b', age=1)
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='b', age=1)
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by()
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by()
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(name='a')
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(name='a')
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(word='a')
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(word='a')
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        Bar.create(name='a', xixi='b', age=1)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='a', age=1)
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='b', age=1)
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by()
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(name='a')
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        Bar.create(name='b', xixi='a', age=1)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='a', age=1)
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='b', age=1)
            self.assertEqual(c, 1)
            self.assertFalse(execute.called)
        bar = Bar.create(name='c', xixi='b', age=1)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='b', age=1)
            self.assertEqual(c, 2)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='b', age=1)
            self.assertEqual(c, 2)
            self.assertFalse(execute.called)
        bar.update(xixi='c')
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='b', age=1)
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cache.count_by(xixi='b', age=1)
            self.assertEqual(c, 1)
            self.assertFalse(execute.called)

    def test_create_cache(self):
        bar = Bar.create(name='b', xixi='a', age=1)
        with no_cache_client(Bar):
            create_cache(bar)
        create_cache(bar)

    def test_add_handler(self):
        bar = Bar.create(name='b', xixi='a', age=1)
        with db.transaction():
            Bar.cache.add_handler(bar)
            Bar.cache.add_handler(None)

    def test_build_report_miss_msg(self):
        msg = Bar.cache._build_report_miss_msg('get_by', 1)
        self.assertEqual(msg, 'Miss cache method invocation: `Bar.get_by(1)`')  # noqa
        msg = Bar.cache._build_report_miss_msg('get_by', c=1)
        self.assertEqual(msg, 'Miss cache method invocation: `Bar.get_by(c=1)`')  # noqa
        msg = Bar.cache._build_report_miss_msg('get_by', 1, c=1, a=2)
        self.assertEqual(msg, 'Miss cache method invocation: `Bar.get_by(1, a=2, c=1)`')  # noqa

    def test_before_create_bug(self):
        class _Lala(Lala):
            __table_name__ = 'lala'

            def before_create(self):
                self.age = 2

        l = _Lala.create(name='a')
        self.assertEqual(l.age, 2)
        l = _Lala.get(l.id)
        self.assertEqual(l.age, 2)
