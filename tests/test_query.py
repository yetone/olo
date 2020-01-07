import json
from datetime import datetime

from olo import funcs
from olo.errors import ExpressionError, SupportError
from olo.funcs import AVG, COUNT, DISTINCT, SUM
from olo.utils import missing

from .base import Dummy, Foo, TestCase
from .fixture import is_pg

attrs = dict(
    name='foo',
    tags=['a', 'b', 'c'],
    payload={
        'abc': [1, 2, 3],
        'def': [4, 5, 6]
    }
)


class TestQuery(TestCase):

    @staticmethod
    def create_dummys():
        return [
            Dummy.create(name='foo', age=1),
            Dummy.create(name='foo', age=2),
            Dummy.create(name='foo', age=3),
            Dummy.create(name='bar', age=2),
        ]

    def test_first(self):
        Dummy.create(**attrs)
        dummy = Dummy.query.first()
        self.assertEqual(dummy.id, 1)
        self.assertEqual(dummy.name, 'foo')
        self.assertTrue(isinstance(dummy.tags, list))
        self.assertTrue(isinstance(dummy.payload, dict))
        self.assertTrue(isinstance(dummy.created_at, datetime))

    def test_all(self):
        dummys = Dummy.query.all()
        self.assertEqual(dummys, [])
        d0 = Dummy.create(**attrs)
        d1 = Dummy.create(**attrs)
        d2 = Dummy.create(**attrs)
        dummys = Dummy.query.all()
        self.assertEqual(len(dummys), 3)
        self.assertEqual(
            {d.id for d in dummys},
            {d0.id, d1.id, d2.id}
        )

    def test_filter(self):
        Dummy.create(**attrs)
        Dummy.create(name='bar')
        Dummy.create(name='xx')
        dummy = Dummy.query.filter(name='bar').first()
        self.assertEqual(dummy.id, 2)
        self.assertEqual(dummy.name, 'bar')
        dummys = Dummy.query.filter(Dummy.name.in_(['bar', 'xx'])).all()
        self.assertEqual(len(dummys), 2)

    def test_multi_filter(self):
        self.create_dummys()
        dummys = Dummy.query.filter(name='foo').filter(age=2).all()
        self.assertEqual(len(dummys), 1)

    def test_specific_entities(self):
        Dummy.create(name='foo0', age=2)
        Dummy.create(name='foo1', age=5)
        Dummy.create(name='bar0', age=3)
        Dummy.create(name='bar0', age=1, tags=['love', 'you'])
        Dummy.create(name='bar1', age=4)
        res = Dummy.query('name', 'age').order_by(Dummy.age.desc()).all()
        self.assertEqual(
            res,
            [
                ('foo1', 5),
                ('bar1', 4),
                ('bar0', 3),
                ('foo0', 2),
                ('bar0', 1),
            ]
        )
        res = Dummy.query(Dummy.id).order_by(Dummy.age.desc()).all()
        self.assertEqual(len(res), 5)
        self.assertEqual(res[-1], 4)
        res = Dummy.query(funcs.COUNT(funcs.DISTINCT(Dummy.name))).all()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0], 4)
        res = Dummy.query('tags', 'name').filter(age=1).one()
        self.assertEqual(res[0], ['love', 'you'])
        self.assertEqual(res[1], 'bar0')
        res = Dummy.query('tags', 'name', raw=True).filter(age=1).one()
        self.assertEqual(res[0], json.dumps(['love', 'you']))
        res = Dummy.query('age').order_by(Dummy.age.desc()).all()
        self.assertEqual(res, [5, 4, 3, 2, 1])
        res = Dummy.query('age').filter(id=4).first()
        self.assertEqual(res, 1)
        res = Dummy.query('age', Dummy, Dummy.id).filter(id=4).first()
        d = Dummy.get(4)
        self.assertEqual(res, (1, d, d.id))

    def test_count(self):
        dummys = self.create_dummys()
        self.assertEqual(Dummy.query.count(), len(dummys))
        self.assertEqual(Dummy.query('id').count(), len(dummys))

    def test_count_join(self):
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
        q = Foo.query.join(Dummy).on(Foo.age == Dummy.age)
        count = q.count()
        self.assertEqual(count, 5)

    def test_count_and_all(self):
        # FIXME(PG)
        if is_pg:
            return
        dummys = self.create_dummys()
        count, items = Dummy.query.order_by('id').count_and_all()
        self.assertEqual(count, len(dummys))
        self.assertEqual([x.id for x in items], [x.id for x in dummys])

    def test_len(self):
        self.create_dummys()
        self.assertEqual(len(Dummy.query), 4)

    def test_offset(self):
        self.create_dummys()
        res = Dummy.query.offset(2).limit(2).all()
        self.assertEqual(res[0].age, 3)

    def test_limit(self):
        Dummy.create(name='foo', age=1)
        Dummy.create(name='foo', age=2)
        Dummy.create(name='foo', age=2)
        Dummy.create(name='bar', age=2)
        res = Dummy.query.limit(3).all()
        self.assertEqual(len(res), 3)

    def test_getitem(self):
        Dummy.create(name='foo', age=1)
        Dummy.create(name='foo', age=2)
        Dummy.create(name='foo', age=2)
        Dummy.create(name='bar', age=2)
        self.assertEqual(Dummy.query[:], Dummy.query.all())
        self.assertEqual(len(Dummy.query[:]), 4)
        self.assertEqual(len(Dummy.query[:2]), 2)
        res = Dummy.query.order_by(Dummy.id.asc())[1:2]
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].id, 2)
        res = Dummy.query[:3]
        self.assertEqual(len(res), 3)
        self.assertEqual(Dummy.get(1), Dummy.query[1])
        self.assertRaises(SupportError, lambda: Dummy.query[::1])

    def test_generator(self):
        Dummy.create(name='foo0', age=3)
        Dummy.create(name='foo0', age=2)
        Dummy.create(name='foo0', age=4)
        Dummy.create(name='foo0', age=1)
        for d in Dummy.query:
            self.assertTrue(isinstance(d, Dummy))
        l = []
        for age in Dummy.query('age').order_by(Dummy.age.desc()):
            l.append(age)
        self.assertEqual(l, [4, 3, 2, 1])

    def test_update(self):
        Dummy.create(name='foo0', age=3)
        Dummy.create(name='foo1', age=2)
        Dummy.create(name='foo2', age=4)
        Dummy.create(name='foo3', age=1)
        Dummy.query.filter(age=4).update(name='bar')
        res = Dummy.query('name').filter(age=4).one()
        self.assertEqual(res, 'bar')
        res = Dummy.query('name').filter(age=3).one()
        self.assertEqual(res, 'foo0')
        Dummy.query.filter(Dummy.age.in_((1, 2))).update(name='hehe')
        res = Dummy.query('age', 'name').order_by(Dummy.age.desc()).all()
        self.assertEqual(res,
                         [(4, 'bar'), (3, 'foo0'), (2, 'hehe'), (1, 'hehe')])
        Dummy.query.filter(name='bar').update(age=Dummy.age + 1)
        res = Dummy.query('age').filter(name='bar').first()
        self.assertEqual(res, 5)
        Dummy.query.filter(
            Dummy.age > 0
        ).order_by('age').limit(1).update(name='xixi')
        res = Dummy.query.order_by('age').all()
        self.assertEqual(res[0].name, 'xixi')
        self.assertNotEqual(res[1].name, 'xixi')
        with self.assertRaises(ExpressionError):
            Dummy.query.update(age=Dummy.age + 1)

    def test_delete(self):
        Dummy.create(name='foo0', age=3)
        Dummy.create(name='foo1', age=2)
        Dummy.create(name='foo2', age=4)
        Dummy.create(name='foo3', age=1)
        Dummy.query.filter(age=4).delete()
        res = Dummy.query.filter(age=4).one()
        self.assertTrue(res is None)
        res = Dummy.query('name').filter(age=3).one()
        self.assertEqual(res, 'foo0')
        Dummy.query.filter(Dummy.age.in_((3, 1))).delete()
        res = Dummy.query('age').all()
        self.assertEqual(res, [2])
        with self.assertRaises(ExpressionError):
            Dummy.query.delete()

    def test_expression(self):
        Dummy.create(name='foo0', age=3)
        q = Dummy.query.filter(name='foo0')
        ast = q._get_expression().get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '=',
            ['COLUMN', 'dummy', 'name'],
            ['VALUE', 'foo0']
        ])
        _q = q.filter(age=3)
        ast = _q._get_expression().get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'AND',
            ['BINARY_OPERATE',
             '=',
             ['COLUMN', 'dummy', 'name'],
             ['VALUE', 'foo0']
             ],
            ['BINARY_OPERATE',
             '=',
             ['COLUMN', 'dummy', 'age'],
             ['VALUE', 3]
             ]
        ])
        ast = q._get_expression().get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '=',
            ['COLUMN', 'dummy', 'name'],
            ['VALUE', 'foo0']
        ])
        q = Dummy.query.filter(name=missing, id=1)
        ast = q._get_expression().get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '=',
            ['COLUMN', 'dummy', 'id'],
            ['VALUE', 1]
        ])

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
        q = Foo.query.join(Dummy).on(Foo.age == Dummy.age)
        res = q.all()
        self.assertEqual(len(res), 5)
        self.assertEqual({x.name for x in res}, {
            'foo2', 'foo3', 'foo4', 'foo5', 'foo6'
        })
        q = Dummy.query.join(Foo).on(Foo.age == Dummy.age)
        res = q.all()
        self.assertEqual(len(res), 5)
        self.assertEqual({x.name for x in res}, {
            'dummy0', 'dummy0', 'dummy1', 'dummy1', 'dummy1'
        })
        q = Dummy.query.join(Foo).on(Foo.age == Dummy.age,
                                     Dummy.age == 6)
        res = q.all()
        self.assertEqual(len(res), 3)
        self.assertEqual({x.name for x in res}, {
            'dummy1', 'dummy1', 'dummy1'
        })
        q = Dummy.query(DISTINCT(Dummy.id)).join(Foo).on(
            Foo.age == Dummy.age
        ).order_by(
            Foo.id.desc(), Dummy.age.desc()
        )
        res = q.all()
        self.assertEqual(res, [2, 1])
        q = Dummy.query(DISTINCT(Dummy.id)).left_join(Foo).on(
            Foo.age == Dummy.age
        ).order_by(
            Foo.id.desc(), Dummy.age.desc()
        )
        res = q.all()
        if is_pg:
            # FIXME(PG)
            self.assertEqual(res, [3, 2, 1])
        else:
            self.assertEqual(res, [2, 1, 3])
        q = Dummy.query(DISTINCT(Dummy.id)).right_join(Foo).on(
            Foo.age == Dummy.age
        ).order_by(
            Foo.id.desc(), Dummy.age.desc()
        )
        res = q.all()
        self.assertEqual(res, [2, 1, None])
        q = Dummy.query(Dummy.id).join(Dummy)
        self.assertEqual(
            q.get_sql_ast(),
            ['SELECT',
             ['SERIES',
              ['COLUMN', 'dummy', 'id']],
             ['FROM',
              ['JOIN', 'INNER',
               ['TABLE', 'dummy'],
               ['TABLE', 'dummy'],
               [],
               ]],
             ]
        )

    def test_for_update(self):
        q = Dummy.query(Dummy.id).for_update()
        self.assertEqual(
            q.get_sql_ast(),
            ['SELECT',
             ['SERIES',
              ['COLUMN', 'dummy', 'id']],
             ['FROM',
              ['TABLE', 'dummy'],
              ],
             ['FOR UPDATE']
             ]
        )

    def test_order_by(self):
        Dummy.create(name='foo0', age=3)
        Dummy.create(name='foo2', age=6)
        Dummy.create(name='foo2', age=7)
        Dummy.create(name='foo3', age=4)
        Dummy.create(name='foo4', age=2)
        rv = Dummy.query('age').order_by('age').all()
        self.assertEqual(rv, [2, 3, 4, 6, 7])
        rv = Dummy.query('age').order_by(Dummy.age).all()
        self.assertEqual(rv, [2, 3, 4, 6, 7])
        rv = Dummy.query('age').order_by(Dummy.age.desc()).all()
        self.assertEqual(rv, [7, 6, 4, 3, 2])
        age = Dummy.age.alias('a')
        rv = Dummy.query(age).order_by(age).all()
        self.assertEqual(rv, [2, 3, 4, 6, 7])
        rv = Dummy.query(age).order_by(age.desc()).all()
        self.assertEqual(rv, [7, 6, 4, 3, 2])
        rv = Dummy.query(age).order_by('age desc').all()
        self.assertEqual(rv, [7, 6, 4, 3, 2])
        rv = Dummy.query(age).order_by(Dummy.id.asc(), Dummy.age.desc()).all()
        self.assertEqual(rv, [3, 6, 7, 4, 2])
        rv = Dummy.query(age).order_by(Dummy.age.in_([2, 4]).desc(), Dummy.id.desc()).all()  # noqa
        self.assertEqual(rv, [2, 4, 7, 6, 3])
        rv = Dummy.query(age).order_by(Dummy.age.in_([2, 4]).desc()).order_by(Dummy.id.desc()).all()  # noqa
        self.assertEqual(rv, [2, 4, 7, 6, 3])

    def test_group_by(self):
        Dummy.create(name='foo0', age=1)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo3', age=3)
        Dummy.create(name='foo4', age=3)
        rv = Dummy.query('age', funcs.COUNT(1)).group_by('age').order_by('age').all()
        self.assertEqual(rv, [(1, 1), (2, 2), (3, 2)])
        rv = Dummy.query('name', 'age').group_by('name', 'age').order_by('age').all()
        self.assertEqual(rv, [('foo0', 1), ('foo2', 2),
                              ('foo3', 3), ('foo4', 3)])
        rv = Dummy.query('name', 'age').group_by('name').group_by('age').order_by('age').all()
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
        rv = Dummy.query('age', c).group_by(
            'age'
        ).having(c > 2).all()
        self.assertEqual(rv, [(3, 3)])

    def test_sum(self):
        Dummy.create(name='foo0', age=1)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo3', age=3)
        rv = Dummy.query(SUM(Dummy.age)).first()
        self.assertEqual(rv, 6)

    def test_avg(self):
        Dummy.create(name='foo0', age=1)
        Dummy.create(name='foo2', age=2)
        Dummy.create(name='foo3', age=3)
        rv = Dummy.query(AVG(Dummy.age)).first()
        self.assertEqual(rv, 2)
