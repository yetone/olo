# coding: utf-8

from olo import select, select_, funcs

from .base import TestCase, Foo, Bar


class TestASTAPI(TestCase):
    def is_same_q(self, q1, q2):
        ast1 = q1.get_sql_ast()
        ast2 = q2.get_sql_ast()
        self.assertEqual(ast1, ast2)

    def test_no_condition(self):
        q1 = select(f for f in Foo)
        q2 = Foo.query
        self.is_same_q(q1, q2)

    def test_entities(self):
        q1 = select(f.age for f in Foo if f.id > 0)
        q2 = Foo.query('age').filter(Foo.id > 0)
        q3 = Foo.query(Foo.age).filter(Foo.id > 0)
        self.is_same_q(q1, q2)
        self.is_same_q(q1, q3)
        q1 = select((f.age, f, f.id) for f in Foo if f.id > 0)
        q2 = Foo.query('age', Foo, 'id').filter(Foo.id > 0)
        q3 = Foo.query(Foo.age, Foo, Foo.id).filter(Foo.id > 0)
        self.is_same_q(q1, q2)
        self.is_same_q(q1, q3)

    def test_multi_entities(self):
        q1 = select((f, f.id, f.age) for f in Foo if f.id > 0)
        q2 = Foo.query(Foo, Foo.id, Foo.age).filter(Foo.id > 0)
        self.is_same_q(q1, q2)

    def test_condition(self):
        q1 = select(f for f in Foo if f.id == 1)
        q2 = Foo.query.filter(id=1)
        self.is_same_q(q1, q2)

    def test_complex_condition(self):
        q1 = select(
            f for f in Foo
            if f.id == 1 and f.age in [1, 2] or f.name == 'a'
        )
        q2 = Foo.query.filter(
            (Foo.id == 1) & (Foo.age.in_((1, 2))) | (
                Foo.name == 'a'
            )
        )
        self.is_same_q(q1, q2)

    def test_join(self):
        q1 = select(
            f for f in Foo
            for b in Bar
            if f.id == b.age and f.age in [1, 2] or b.name == 'a'
        )
        q2 = Foo.query.join(Bar).on(
            (Foo.id == Bar.age) & (Foo.age.in_((1, 2))) | (
                Bar.name == 'a'
            )
        )
        self.is_same_q(q1, q2)

    def test_funcs(self):
        q1 = select(
            f for f in Foo
            if f.id < max(f.id for f in Foo)
        )
        q2 = Foo.query.filter(
            Foo.id < Foo.query(funcs.MAX(Foo.id))
        )
        self.is_same_q(q1, q2)
        q1 = select_(
            f for f in Foo
            if f.id < max(f.id for f in Foo)
        )
        q2 = Foo.query.filter(
            Foo.id < Foo.query(funcs.MAX(Foo.id))
        )
        self.is_same_q(q1, q2)
        q1 = select(
            f.id.distinct().count() for f in Foo
            if f.id < 1 and f.name == 'foo'
        )
        q2 = select(
            funcs.COUNT(funcs.DISTINCT(f.id)) for f in Foo
            if f.id < 1 and f.name == 'foo'
        )
        q3 = Foo.query(funcs.COUNT(funcs.DISTINCT(Foo.id))).filter(
            (Foo.id < 1) & (Foo.name == 'foo')
        )
        self.is_same_q(q1, q2)
        self.is_same_q(q2, q3)

    def test_func(self):
        q1 = Foo.query(funcs.COUNT(funcs.DISTINCT(Foo.id))).filter(
            (Foo.id < 1) & (Foo.name == 'foo')
        )

        @select
        def q2():
            for f in Foo:
                if f.id < 1 and f.name == 'foo':
                    yield funcs.COUNT(funcs.DISTINCT(Foo.id))

        self.is_same_q(q1, q2)

    def test_op_apply(self):
        args = [1]
        q1 = select(f for f in Foo if f.id == args[0])
        q2 = Foo.query.filter(id=args[0])
        self.is_same_q(q1, q2)
