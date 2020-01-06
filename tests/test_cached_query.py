# coding: utf-8
from olo import funcs
from olo.funcs import COUNT, SUM, AVG, MAX, DISTINCT
from .base import TestCase, Foo, Bar, Dummy
from .fixture import is_pg
from .utils import (
    patched_execute, no_pk
)


attrs = dict(
    name='foo',
    tags=['a', 'b', 'c'],
    password='password',
    payload={
        'abc': ['1', 2, 3],
        'def': [4, '5', 6]
    }
)


class TestCachedQuery(TestCase):

    def test_fallback(self):
        bar = Bar.create(name='a', xixi='a', age=1)
        with patched_execute as execute:
            bar = Bar.cq.filter(age=MAX(Bar.cq('age'))).first()
            self.assertIsNotNone(bar)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bar = Bar.cq.filter(age=MAX(Bar.cq('age'))).first()
            self.assertIsNotNone(bar)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bar = Bar.cq.filter(Bar.age > 0).first()
            self.assertIsNotNone(bar)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bar = Bar.cq.filter(Bar.age > 0).first()
            self.assertIsNotNone(bar)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bar = Bar.cq('age').filter(Bar.age > 0).first()
            self.assertIsNotNone(bar)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bar = Bar.cq('age').filter(Bar.age > 0).first()
            self.assertIsNotNone(bar)
            self.assertTrue(execute.called)

    def test_first(self):
        with patched_execute as execute:
            bar = Bar.cq.filter(xixi='a', age=1).first()
            self.assertIsNone(bar)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bar = Bar.cq.filter(xixi='a', age=1).first()
            self.assertIsNone(bar)
            self.assertFalse(execute.called)

        bar = Bar.create(name='a', xixi='a', age=1)
        with patched_execute as execute:
            bar = Bar.cq.filter(xixi='a', age=1).first()
            self.assertIsNotNone(bar)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bar = Bar.cq.filter(xixi='a', age=1).first()
            self.assertIsNotNone(bar)
            self.assertFalse(execute.called)

    def test_all(self):
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).all()
            self.assertEqual(bars, [])
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).all()
            self.assertEqual(bars, [])
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(10).all()
            self.assertEqual(bars, [])
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(bars, [])
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.limit(10).all()
            self.assertEqual(bars, [])
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.limit(11).all()
            self.assertEqual(bars, [])
            self.assertFalse(execute.called)
        bar = Bar.create(name='a', xixi='a', age=1)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
            self.assertEqual(execute.call_count, 2)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.limit(10).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
        bar.update(name='a+')
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
            self.assertEqual(execute.call_count, 2)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        bar.update(name='a')
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
            self.assertEqual(execute.call_count, 2)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        bar.update(word='1')
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
            self.assertEqual(execute.call_count, 1)
            self.assertEqual(bars[0].word, bar.word)
        bar.update(word='2')
        Bar.cache.get(bar.name)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
            self.assertEqual(bars[0].word, bar.word)
        bar.update(xixi='b')
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='a', age=1).limit(11).all()
            self.assertEqual(len(bars), 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).limit(11).all()
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        bar.update(word='a')
        bar = Bar.create(name='b', xixi='b', age=1, word='b')
        bar = Bar.create(name='c', xixi='b', age=1, word='c')
        bar = Bar.create(name='d', xixi='b', age=1, word='d')
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).limit(11).all()
            self.assertEqual(len(bars), 4)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).limit(11).all()
            self.assertEqual(len(bars), 4)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).limit(2).all()
            self.assertEqual(len(bars), 2)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cache.gets_by(xixi='b', age=1, start=3,
                                     limit=2)
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                '-name'
            ).limit(3).all()
            self.assertEqual(len(bars), 3)
            self.assertEqual(['d', 'c', 'b'], list(map(lambda x: x.name, bars)))
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                '-name'
            ).limit(3).all()
            self.assertEqual(len(bars), 3)
            self.assertEqual(['d', 'c', 'b'], list(map(lambda x: x.name, bars)))
            self.assertFalse(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                'name'
            ).limit(3).all()
            self.assertEqual(len(bars), 3)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                'name'
            ).limit(3).all()
            self.assertEqual(len(bars), 3)
            self.assertEqual(['a', 'b', 'c'], list(map(lambda x: x.name, bars)))
            self.assertFalse(execute.called)

        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                '-age', 'word'
            ).offset(3).limit(2).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)

        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                '-age', 'word'
            ).offset(3).limit(2).all()
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)

        _bar = bars[0]
        _bar.update(xixi='c')

        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                '-age', 'word'
            ).offset(2).limit(2).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)

        _bar.update(xixi='b')

        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                'word', 'age'
            ).offset(3).limit(2).all()
            self.assertEqual(len(bars), 1)
            self.assertTrue(execute.called)

        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                'word', 'age'
            ).offset(3).limit(2).all()
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)

        Bar.create(name='e', xixi='b', age=1, word='e')
        Bar.create(name='f', xixi='b', age=1, word='f')

        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                'word', 'age'
            ).offset(3).limit(2).all()
            self.assertEqual(len(bars), 2)
            self.assertTrue(execute.called)

        with patched_execute as execute:
            bars = Bar.cq.filter(xixi='b', age=1).order_by(
                'word', 'age'
            ).offset(3).limit(2).all()
            self.assertEqual(len(bars), 2)
            self.assertFalse(execute.called)

        with patched_execute as execute:
            bars = Bar.cq.filter(name='e').all()
            self.assertEqual(len(bars), 1)
            self.assertFalse(execute.called)

        Foo.create(name='1', age=1)
        Foo.create(name='2', age=1)
        Foo.create(name='3', age=2)

        with no_pk(Foo):
            Foo.cq.filter(age=1).limit(3).all()

        foos = Foo.cq.filter(age=3).limit(3).all()
        self.assertEqual(foos, [])

    def test_count_by(self):
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='a', age=1).count()
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='a', age=1).count()
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='b', age=1).count()
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='b', age=1).count()
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter().count()
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter().count()
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(name='a').count()
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(name='a').count()
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(word='a').count()
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(word='a').count()
            self.assertEqual(c, 0)
            self.assertTrue(execute.called)
        Bar.create(name='a', xixi='b', age=1)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='a', age=1).count()
            self.assertEqual(c, 0)
            self.assertFalse(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='b', age=1).count()
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter().count()
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(name='a').count()
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        Bar.create(name='b', xixi='a', age=1)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='a', age=1).count()
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='b', age=1).count()
            self.assertEqual(c, 1)
            self.assertFalse(execute.called)
        bar = Bar.create(name='c', xixi='b', age=1)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='b', age=1).count()
            self.assertEqual(c, 2)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='b', age=1).count()
            self.assertEqual(c, 2)
            self.assertFalse(execute.called)
        bar.update(xixi='c')
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='b', age=1).count()
            self.assertEqual(c, 1)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            c = Bar.cq.filter(xixi='b', age=1).count()
            self.assertEqual(c, 1)
            self.assertFalse(execute.called)

    def test_order_by(self):
        Dummy.create(name='foo0', age=3)
        Dummy.create(name='foo2', age=6)
        Dummy.create(name='foo2', age=7)
        Dummy.create(name='foo3', age=4)
        Dummy.create(name='foo4', age=2)
        rv = Dummy.cq('age').order_by('age').all()
        self.assertEqual(rv, [2, 3, 4, 6, 7])
        rv = Dummy.cq('age').order_by(Dummy.age).all()
        self.assertEqual(rv, [2, 3, 4, 6, 7])
        rv = Dummy.cq('age').order_by(Dummy.age.desc()).all()
        self.assertEqual(rv, [7, 6, 4, 3, 2])
        age = Dummy.age.alias('a')
        rv = Dummy.cq(age).order_by(age).all()
        self.assertEqual(rv, [2, 3, 4, 6, 7])
        rv = Dummy.cq(age).order_by(age.desc()).all()
        self.assertEqual(rv, [7, 6, 4, 3, 2])
        rv = Dummy.cq(age).order_by(Dummy.id.asc(), Dummy.age.desc()).all()
        self.assertEqual(rv, [3, 6, 7, 4, 2])
        rv = Dummy.cq(age).order_by(Dummy.age.in_([2, 4]).desc(), Dummy.id.desc()).all()  # noqa
        self.assertEqual(rv, [2, 4, 7, 6, 3])
        rv = Dummy.cq(age).order_by(Dummy.age.in_([2, 4]).desc()).order_by(Dummy.id.desc()).all()  # noqa
        self.assertEqual(rv, [2, 4, 7, 6, 3])

    def test_group_by(self):
        Dummy.create(name='foo0', age=1)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo3', age=3)
        Dummy.create(name='foo4', age=3)
        rv = Dummy.cq('age', funcs.COUNT(1)).group_by('age').order_by('age').all()
        self.assertEqual(rv, [(1, 1), (2, 2), (3, 2)])
        rv = Dummy.cq('name', 'age').group_by('name', 'age').order_by('age').all()
        self.assertEqual(rv, [('foo0', 1), ('foo2', 2),
                              ('foo3', 3), ('foo4', 3)])
        rv = Dummy.cq('name', 'age').group_by('name').group_by('age').order_by('age').all()
        self.assertEqual(rv, [('foo0', 1), ('foo2', 2),
                              ('foo3', 3), ('foo4', 3)])

    def test_having(self):
        # FIXME(PG)
        if is_pg:
            return
        Dummy.create(name='foo0', age=1)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo3', age=3)
        Dummy.create(name='foo4', age=3)
        Dummy.create(name='foo5', age=3)
        c = COUNT(1).alias('c')
        rv = Dummy.cq('age', c).group_by(
            'age'
        ).having(c > 2).all()
        self.assertEqual(rv, [(3, 3)])

    def test_join(self):
        Dummy.create(name='dummy0', age=3)
        Dummy.create(name='dummy1', age=6)
        Dummy.create(name='dummy2', age=9)
        Foo.create(name='foo0', age=1)
        Foo.create(name='foo1', age=2)
        Foo.create(name='foo2', age=3)
        Foo.create(name='foo3', age=3)
        Foo.create(name='foo4', age=6)
        Foo.create(name='foo5', age=6)
        Foo.create(name='foo6', age=6)
        q = Foo.cq.join(Dummy).on(Foo.age == Dummy.age)
        res = q.all()
        self.assertEqual(len(res), 5)
        self.assertEqual({x.name for x in res}, {
            'foo2', 'foo3', 'foo4', 'foo5', 'foo6'
        })
        q = Dummy.cq.join(Foo).on(Foo.age == Dummy.age)
        res = q.all()
        self.assertEqual(len(res), 5)
        self.assertEqual({x.name for x in res}, {
            'dummy0', 'dummy0', 'dummy1', 'dummy1', 'dummy1'
        })
        q = Dummy.cq.join(Foo).on(Foo.age == Dummy.age,
                                  Dummy.age == 6)
        res = q.all()
        self.assertEqual(len(res), 3)
        self.assertEqual({x.name for x in res}, {
            'dummy1', 'dummy1', 'dummy1'
        })
        q = Dummy.cq(DISTINCT(Dummy.id)).join(Foo).on(
            Foo.age == Dummy.age
        ).order_by(
            Foo.id.desc(), Dummy.age.desc()
        )
        res = q.all()
        self.assertEqual(res, [2, 1])
        q = Dummy.cq(DISTINCT(Dummy.id)).left_join(Foo).on(
            Foo.age == Dummy.age
        ).order_by(
            Foo.id.desc(), Dummy.age.desc()
        )
        res = q.all()
        if is_pg:
            self.assertEqual(res, [3, 2, 1])
        else:
            self.assertEqual(res, [2, 1, 3])
        q = Dummy.cq(DISTINCT(Dummy.id)).right_join(Foo).on(
            Foo.age == Dummy.age
        ).order_by(
            Foo.id.desc(), Dummy.age.desc()
        )
        res = q.all()
        self.assertEqual(res, [2, 1, None])

    def test_sum(self):
        Dummy.create(name='foo0', age=1)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo3', age=3)
        rv = Dummy.cq(SUM(Dummy.age)).first()
        self.assertEqual(rv, 6)

    def test_avg(self):
        Dummy.create(name='foo0', age=1)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo3', age=3)
        rv = Dummy.cq(AVG(Dummy.age)).first()
        self.assertEqual(rv, 2)
