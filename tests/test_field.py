from typing import Optional, List

from olo.field import ConstField, UnionField, BatchField
from olo.errors import ValidationError, DbFieldVersionError
from olo.compat import xrange

from .base import TestCase, Dummy, Foo, Gender, Foo as _Foo, Bar
from .utils import patched_db_get, patched_db_get_multi, patched_execute


class TestField(TestCase):
    def test_default(self):
        d = Dummy.create()
        self.assertEqual(d.age, 12)

    def test_automic(self):
        f = Foo.create()
        self.assertEqual(f.age, 1)
        f.update(age=Foo.age + 1)
        self.assertEqual(f.age, 2)
        f.age = Foo.age + 1
        f.save()
        self.assertEqual(f.age, 3)

    def test_output(self):
        foo = Foo.create(age_str=1)
        self.assertEqual(foo.age_str, '1')

    def test_deparse(self):
        v = Foo.age.deparse(1)
        self.assertEqual(v, '1')

        class V:
            def __str__(self):
                raise ValueError

        _v = V()
        v = Foo.age.deparse(_v)
        self.assertEqual(v, _v)

    def test_validate(self):
        choices = Foo.age.choices
        Foo.age.choices = None
        Foo.age.validate(234)
        Foo.age.choices = [1]
        try:
            with self.assertRaises(ValidationError):
                Foo.age.validate(2)
        finally:
            Foo.age.choices = choices

    def test_sub(self):
        exp = Foo.age - 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '-',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_mul(self):
        exp = Foo.age * 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '*',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_div(self):
        exp = Foo.age / 2
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '/',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 2]
        ])

    def test_mod(self):
        exp = Foo.age % 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '%',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_eq(self):
        exp = Foo.age == 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '=',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])
        exp = Foo.age == None  # noqa
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'IS',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', None]
        ])

    def test_ne(self):
        exp = Foo.age != 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '!=',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])
        exp = Foo.age != None  # noqa
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'IS NOT',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', None]
        ])

    def test_gt(self):
        exp = Foo.age > 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '>',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_ge(self):
        exp = Foo.age >= 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '>=',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_lt(self):
        exp = Foo.age < 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '<',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_le(self):
        exp = Foo.age <= 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '<=',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_not_in_(self):
        exp = Foo.age.not_in_([1])
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'NOT IN',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', (1,)]
        ])
        f = UnionField(Foo.age, Foo.id)
        exp = f.not_in_([1])
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'NOT IN',
            ['BRACKET',
             ['COLUMN', 'foo', 'age'],
             ['COLUMN', 'foo', 'id']],
            ['VALUE', (1,)]
        ])

    def test_like_(self):
        exp = Foo.age.like_(1)
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'LIKE',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_ilike_(self):
        exp = Foo.age.ilike_(1)
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'ILIKE',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_regexp_(self):
        exp = Foo.age.regexp_(1)
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'REGEXP',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_between_(self):
        exp = Foo.age.between_(1)
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'BETWEEN',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_concat_(self):
        exp = Foo.age.concat_(1)
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '||',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_is_(self):
        exp = Foo.age.is_(1)
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'IS',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_is_not_(self):
        exp = Foo.age.is_not_(1)
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'IS NOT',
            ['COLUMN', 'foo', 'age'],
            ['VALUE', 1]
        ])

    def test_get_model(self):
        model_ref = Foo.age._model_ref
        try:
            model = Foo.age.get_model()
            self.assertEqual(model, Foo)
            Foo.age._model_ref = None
            model = Foo.age.get_model()
            self.assertEqual(model, None)
        finally:
            Foo.age._model_ref = model_ref

    def test_table_name(self):
        model_ref = Foo.age._model_ref
        try:
            table_name = Foo.age.table_name
            self.assertEqual(table_name, 'foo')
            Foo.age._model_ref = None
            table_name = Foo.age.table_name
            self.assertEqual(table_name, None)
        finally:
            Foo.age._model_ref = model_ref

    def test_enum(self):
        d = Dummy.create(gender='MALE')
        self.assertEqual(d.gender, Gender.MALE)


class TestConstField(TestCase):
    def test_sqlize(self):
        v = 1
        f = ConstField(v)
        self.assertEqual(
            f.get_sql_ast(),
            ['VALUE', v],
        )
        v = 'a'
        f = ConstField(v)
        self.assertEqual(
            f.get_sql_ast(),
            ['VALUE', v],
        )


class TestDbField(TestCase):
    def test_get_v0(self):
        ver = Foo.prop1.version
        f = Foo.create(prop1=[1])
        f._data.pop('prop1')
        db = f._get_db()
        uuid = f.get_finally_uuid()
        db.db_set(uuid, '{"prop1": [2]}')
        try:
            Foo.prop1.version = 0
            self.assertEqual(f.prop1, [2])
        finally:
            Foo.prop1.version = ver

    def test_set_v0(self):
        ver = Foo.prop1.version
        f = Foo.create(prop1=[1])
        f._data.pop('prop1')
        db = f._get_db()
        uuid = f.get_finally_uuid()
        db.db_set(uuid, '{"prop1": [2]}')
        try:
            Foo.prop1.version = 0
            f.prop1 = [1]
            f.save()
            self.assertEqual(f.prop1, [1])
        finally:
            Foo.prop1.version = ver

    def test_set_v1(self):
        f = Foo.create(prop1=[1])
        self.assertEqual(f.prop1, [1])
        f.update(prop1=None)
        self.assertIsNone(f.prop1)

    def test__get__(self):
        ver = Foo.prop1.version
        f = Foo.create(prop1=[1])
        self.assertEqual(f.prop1, [1])
        f = Foo.get(f.id)
        try:
            Foo.prop1.version = 999
            with self.assertRaises(DbFieldVersionError):
                f.prop1
        finally:
            Foo.prop1.version = ver

    def test__set__(self):
        f = Foo.create(prop1=[1])
        f.prop1 = [2]
        f.save()
        self.assertEqual(f.prop1, [2])
        f = Foo.get(f.id)
        self.assertEqual(f.prop1, [2])
        bu = f.before_update
        try:
            f.before_update = lambda **kwargs: False
            f.prop1 = [3]
            f.save()
            self.assertEqual(f.prop1, [2])
        finally:
            f.before_update = bu
        ver = Foo.prop1.version
        try:
            Foo.prop1.version = 999
            with self.assertRaises(DbFieldVersionError):
                Foo.prop1.db_set(f, [3])
        finally:
            Foo.prop1.version = ver

    def test__delete__(self):
        f = Foo.create(prop1=[1])
        bu = f.before_update
        try:
            f.before_update = lambda **kwargs: False
            del f.prop1
            self.assertEqual(f.prop1, [1])
        finally:
            f.before_update = bu

    def test_prefetch(self):
        for i in xrange(1, 8):
            Dummy.create(count=i, prop1=[i])

        ds = Dummy.gets_by()
        with patched_db_get as db_get:
            with patched_db_get_multi as db_get_multi:
                for d in ds:
                    self.assertEqual(d.count, d.id)
                    self.assertEqual(d.prop1, [str(d.id)])

                    # visit `count1` field multi times
                    for j in xrange(5):
                        self.assertIsNone(d.count1)

                self.assertEqual(db_get.call_count, 0)
                self.assertEqual(db_get_multi.call_count, 4)


class TestBatchField(TestCase):

    def test_batch_field(self):
        class Foo(_Foo):

            a_bar = BatchField(Bar)
            b_bar = BatchField(Bar)
            c_bars = BatchField(List[Bar])
            d_foo = BatchField(lambda: Foo)

            @a_bar.getter
            @classmethod
            def get_a_bars(cls, foos):
                bars = Bar.gets([str(f.id) for f in foos])
                return bars

            @b_bar.getter
            @classmethod
            def get_b_bars(cls, foos):
                bars = Bar.gets([str(f.id) for f in foos])
                return {
                    int(b.name): b
                    for b in bars
                }

            @c_bars.getter
            @classmethod
            def get_c_bars_list(cls, foos):
                bars = Bar.gets([str(f.id) for f in foos])
                return [[b] for b in bars]

            @d_foo.getter
            @classmethod
            def get_d_foos(cls, foos):
                return foos

        Foo.create(age=1)
        Foo.create(age=2)
        Foo.create(age=3)
        Foo.create(age=4)
        Foo.create(age=5)
        Foo.create(age=6)
        Bar.create(name=1)
        Bar.create(name=2)
        Bar.create(name=3)

        def a_func():
            fs = Foo.gets_by()
            for f in fs:
                b = f.a_bar

                if f.id > 3:
                    self.assertIsNone(b)
                else:
                    self.assertIsInstance(b, Bar)
                    self.assertEqual(str(f.id), b.name)

        def b_func():
            fs = Foo.gets_by()
            for f in fs:
                b = f.b_bar
                if f.id > 3:
                    self.assertIsNone(b)
                else:
                    self.assertIsInstance(b, Bar)
                    self.assertEqual(str(f.id), b.name)

        def c_func():
            fs = Foo.gets_by()
            for f in fs:
                bs = f.c_bars
                if f.id > 3:
                    self.assertIsNone(bs)
                else:
                    self.assertIsInstance(bs, List)
                    self.assertEqual(str(f.id), bs[0].name)

        def d_func():
            fs = Foo.gets_by()
            for f in fs:
                f_ = f.d_foo
                self.assertIsInstance(f_, Foo)
                self.assertEqual(f.id, f_.id)

        with patched_execute as exe:
            a_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            b_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            c_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            d_func()
            self.assertEqual(exe.call_count, 1)
