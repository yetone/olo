from olo.ext.hybrid import hybrid_property
from tests.base import Dummy as _Dummy
from tests.base import Foo as _Foo
from tests.base import TestCase
from tests.utils import patched_execute


class Dummy(_Dummy):
    __index_keys__ = (
        ('name', 'new_age'),
    )

    @hybrid_property
    def new_age(self):
        return self.age - self.id

    @hybrid_property
    def is_a(self):
        return self.age.in_([1, 2])

    @hybrid_property
    def is_b(self):
        return self.age.not_in_([1, 2])

    @hybrid_property
    def is_c(self):
        return self.foo.is_(None)

    @hybrid_property
    def is_d(self):
        return self.foo.is_not_(None)


class TestHybrid(TestCase):

    def test_hybrid_property(self):
        class Foo(_Foo):
            @hybrid_property
            def new_age(self):
                return self.age - self.id

        q = Foo.query.filter(Foo.new_age > 2)
        sql_ast = q.get_sql_ast()
        self.assertEqual(
            sql_ast,
            ['SELECT',
             ['SERIES',
              ['COLUMN', 'foo', 'id'],
              ['COLUMN', 'foo', 'name'],
              ['COLUMN', 'foo', 'age'],
              ['COLUMN', 'foo', 'age_str'],
              ['COLUMN', 'foo', 'key'],
              ['COLUMN', 'foo', 'boolean'],
              ['COLUMN', 'foo', 'test_getter'],
              ['COLUMN', 'foo', 'test_setter']
              ],
             ['FROM',
              ['TABLE', 'foo'],
              ],
             ['WHERE',
              ['BINARY_OPERATE',
               '>',
               ['BINARY_OPERATE',
                '-',
                ['COLUMN', 'foo', 'age'],
                ['COLUMN', 'foo', 'id']],
               ['VALUE', 2]]]]
        )
        Foo.create(age=3)
        foo = Foo.query.filter(Foo.new_age == 2).first()
        self.assertEqual(foo.new_age, 2)
        d1 = Dummy.create(age=1)
        d2 = Dummy.create(age=2, foo=1)
        d3 = Dummy.create(age=3, foo=2)
        d4 = Dummy.create(age=4)
        dummys = Dummy.gets_by(is_a=True)
        self.assertEqual([d1.id, d2.id], [d.id for d in dummys])
        self.assertTrue(dummys[0].is_a)
        dummys = Dummy.gets_by(is_b=True)
        self.assertEqual([d3.id, d4.id], [d.id for d in dummys])
        self.assertTrue(dummys[0].is_b)
        dummys = Dummy.gets_by(is_c=True)
        self.assertEqual([d1.id, d4.id], [d.id for d in dummys])
        self.assertTrue(dummys[0].is_c)
        dummys = Dummy.gets_by(is_d=True)
        self.assertEqual([d2.id, d3.id], [d.id for d in dummys])
        self.assertTrue(dummys[0].is_d)
        with self.assertRaises(AttributeError):
            foo.new_age = 1
        with self.assertRaises(AttributeError):
            del foo.new_age

        class Foo(_Foo):
            @hybrid_property
            def new_age(self):
                return self.age - self.id

            @new_age.setter
            def new_age(self, value):
                self.age = self.id + value

            @new_age.deleter
            def new_age(self):
                self.age = 0

        foo = Foo.query.filter(Foo.new_age == 2).first()
        foo.new_age = 1
        self.assertEqual(foo.new_age, 1)
        del foo.new_age
        self.assertEqual(foo.new_age, -1)

    def test_hybrid_property_cache(self):

        Dummy.create(name='a', age=1)
        Dummy.create(name='a', age=2)
        Dummy.create(name='a', age=3)
        Dummy.create(name='b', age=4)

        with patched_execute as exe:
            dummys = Dummy.cache.gets_by(name='b', new_age=0, limit=10)
            self.assertEqual(len(dummys), 1)
            self.assertEqual(dummys[0].name, 'b')
            self.assertEqual(dummys[0].new_age, 0)
            self.assertTrue(exe.called)

        with patched_execute as exe:
            dummys = Dummy.cache.gets_by(name='b', new_age=0, limit=10)
            self.assertEqual(len(dummys), 1)
            self.assertEqual(dummys[0].name, 'b')
            self.assertEqual(dummys[0].new_age, 0)
            self.assertFalse(exe.called)
