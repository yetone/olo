from olo.field import UnionField
from olo.context import field_verbose_context

from .base import TestCase, Dummy, Foo


class TestBinaryExpression(TestCase):
    def test_expression(self):
        with field_verbose_context(True):
            cond = Dummy.id == Dummy.age
            sql, params = cond.get_sql_and_params()
            self.assertEqual(sql, '`dummy`.`id` = `dummy`.`age`')
            self.assertEqual(params, [])
            cond = Dummy.id > 2
            sql, params = cond.get_sql_and_params()
            self.assertEqual(sql, '`dummy`.`id` > %s')
            self.assertEqual(params, [2])
            cond = Dummy.id << [1, 2]
            sql, params = cond.get_sql_and_params()
            self.assertEqual(sql, '`dummy`.`id` IN %s')
            self.assertEqual(params, [[1, 2]])
            cond = (Dummy.id > 1) & ((Dummy.age == 3) | (Dummy.age == 2))
            sql, params = cond.get_sql_and_params()
            self.assertEqual(
                sql, '`dummy`.`id` > %s AND (`dummy`.`age` = %s OR `dummy`.`age` = %s)'  # noqa
            )
            self.assertEqual(params, [1, 3, 2])
            cond = (Dummy.id > 2) & (Dummy.age == 3) | (Dummy.age == 1)
            sql, params = cond.get_sql_and_params()
            self.assertEqual(
                sql, '`dummy`.`id` > %s AND `dummy`.`age` = %s OR `dummy`.`age` = %s'  # noqa
            )
            self.assertEqual(params, [2, 3, 1])
            cond = ((Dummy.age == 3) | (Dummy.age == 2)) & (Dummy.id > 2)
            sql, params = cond.get_sql_and_params()
            self.assertEqual(
                sql, '(`dummy`.`age` = %s OR `dummy`.`age` = %s) AND `dummy`.`id` > %s'  # noqa
            )
            self.assertEqual(params, [3, 2, 2])
            cond = (Dummy.id > 2) & ((Dummy.age == 3) | (Dummy.age == 2)) & (Dummy.id < 4)  # noqa
            sql, params = cond.get_sql_and_params()
            self.assertEqual(
                sql, '`dummy`.`id` > %s AND (`dummy`.`age` = %s OR `dummy`.`age` = %s) AND `dummy`.`id` < %s'  # noqa
            )
            self.assertEqual(params, [2, 3, 2, 4])
        cond = Dummy.id == Dummy.age
        sql, params = cond.get_sql_and_params()
        self.assertEqual(sql, '`id` = `age`')
        cond = ((Dummy.age == 3) | (Dummy.age == 2)) & (Dummy.id > 2)  # noqa
        sql, params = cond.get_sql_and_params()
        self.assertEqual(
            sql, '(`age` = %s OR `age` = %s) AND `id` > %s'  # noqa
        )
        cond = ((Dummy.age == 3) | (Dummy.age == 2)) & ((Dummy.id > 2) | (Dummy.id < 1))  # noqa
        sql, params = cond.get_sql_and_params()
        self.assertEqual(
            sql, '(`age` = %s OR `age` = %s) AND (`id` > %s OR `id` < %s)'  # noqa
        )
        exp = Dummy.age == Dummy.age + 1
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, '`age` = `age` + %s')
        self.assertEqual(params, [1])
        exp = Dummy.age == 1
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, '`age` = %s')
        self.assertEqual(params, [1])
        exp = Dummy.age.in_([1, 2])
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, '`age` IN %s')
        self.assertEqual(params, [[1, 2]])
        a = Dummy.age.alias('xixi')
        exp = a == 1
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, 'xixi = %s')
        self.assertEqual(params, [1])
        a = Dummy.age.asc()
        sql, params = a.get_sql_and_params()
        self.assertEqual(sql, '`age` ASC')
        a = Dummy.age == 1
        exp = a.asc()
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, '`age` = %s ASC')
        self.assertEqual(params, [1])
        f = UnionField(Dummy.age, Dummy.id)
        exp = f.not_in_([1])
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, '(`age`, `id`) NOT IN %s')
        exp = Dummy.id.in_(Foo.query('id').filter(Foo.age > 2))
        sql, params = exp.get_sql_and_params()
        self.assertEqual(
            sql,
            '`id` IN (SELECT `id` FROM `foo` WHERE `age` > %s )'
        )
        self.assertEqual(params, [2])

    def test_const(self):
        exp0 = Dummy.id > 100
        exp = exp0 | 1
        sql, params = exp.get_sql_and_params()
        self.assertEqual(
            sql,
            '`id` > %s OR %s != %s'
        )
        self.assertEqual(params, [100, 1, 0])
        exp = exp0 | 'a'
        sql, params = exp.get_sql_and_params()
        self.assertEqual(
            sql,
            "`id` > %s OR LENGTH(%s) > %s"
        )
        self.assertEqual(params, [100, 'a', 0])
        exp = exp0 | True
        sql, params = exp.get_sql_and_params()
        self.assertEqual(
            sql,
            "`id` > %s OR %s"
        )
        self.assertEqual(params, [100, 1])
        self.assertRaises(TypeError, lambda: exp0 | [])

    def test_optimize(self):
        exp = Dummy.age.in_([1, 2]) == False  # noqa
        self.assertEqual(
            exp.get_sql_and_params(),
            ('`age` NOT IN %s', [[1, 2]])
        )
        exp = Dummy.age.in_([1, 2]) == True  # noqa
        self.assertEqual(
            exp.get_sql_and_params(),
            ('`age` IN %s', [[1, 2]])
        )
        exp = Dummy.age.not_in_([1, 2]) == False  # noqa
        self.assertEqual(
            exp.get_sql_and_params(),
            ('`age` IN %s', [[1, 2]])
        )
        exp = (Dummy.age == 1) == False  # noqa
        self.assertEqual(
            exp.get_sql_and_params(),
            ('`age` != %s', [1])
        )
