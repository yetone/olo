import math

from olo import funcs
from olo.errors import SupportError

from .base import TestCase, Dummy, Bar


class TestFuncs(TestCase):
    def test_count(self):
        self.assertEqual(
            funcs.COUNT(Dummy.id).get_sql_and_params()[0],
            'COUNT(`id`)'
        )
        self.assertEqual(
            funcs.COUNT(1).get_sql_and_params()[0],
            'COUNT(1)'
        )
        self.assertEqual(
            funcs.COUNT(Dummy).get_sql_and_params()[0],
            'COUNT(1)'
        )
        self.assertEqual(
            funcs.COUNT(Dummy.id > 2).get_sql_and_params(),
            ('COUNT(CASE WHEN `id` > %s THEN %s ELSE %s END)', [2, 1, None])
        )
        self.assertEqual(
            funcs.COUNT(Dummy.query).get_sql_and_params()[0],
            'SELECT COUNT(1) FROM `dummy` '
        )
        self.assertEqual(
            funcs.COUNT(Dummy.query.filter(id=1)).get_sql_and_params()[0],
            'SELECT COUNT(1) FROM `dummy` WHERE `id` = %s '
        )
        c = funcs.COUNT(1).alias('c')
        exp = c > 1
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, 'c > %s')
        self.assertEqual(params, [1])
        exp = Dummy.id == c
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, '`id` = c')
        self.assertEqual(params, [])
        c = funcs.COUNT(1)
        exp = c > 1
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, 'COUNT(1) > %s')
        exp = Dummy.id == c
        sql, params = exp.get_sql_and_params()
        self.assertEqual(sql, '`id` = COUNT(1)')

    def test_sum(self):
        self.assertEqual(
            funcs.SUM(Dummy.id).get_sql_and_params()[0],
            'SUM(`id`)'
        )
        self.assertEqual(
            funcs.SUM(Dummy.id > 2).get_sql_and_params()[0],
            'SUM(`id` > %s)'
        )
        self.assertEqual(
            funcs.SUM(Dummy.id > 2).get_sql_and_params()[1],
            [2]
        )

    def test_distinct(self):
        self.assertEqual(
            funcs.DISTINCT(Dummy.id).get_sql_and_params()[0],
            'DISTINCT(`id`)'
        )

    def test_avg(self):
        self.assertEqual(
            funcs.AVG(Dummy.id).get_sql_and_params()[0],
            'AVG(`id`)'
        )

    def test_sqrt(self):
        self.assertEqual(
            funcs.SQRT(Dummy.id).get_sql_and_params()[0],
            'SQRT(`id`)'
        )
        self.assertEqual(
            funcs.SQRT(2),
            math.sqrt(2)
        )

    def test_max(self):
        self.assertEqual(
            funcs.MAX(Dummy.id).get_sql_and_params()[0],
            'MAX(`id`)'
        )
        self.assertEqual(
            funcs.MAX(1, 2),
            max(1, 2)
        )
        self.assertRaises(SupportError, lambda: funcs.MAX(Dummy))
        self.assertRaises(SupportError, lambda: funcs.MAX(Dummy.query))
        self.assertRaises(SupportError, lambda: funcs.MAX(Dummy.cq))
        self.assertRaises(SupportError, lambda: funcs.MAX(Bar.query('id')))
        self.assertEqual(
            funcs.MAX(Dummy.query('id')).get_sql_and_params()[0],
            'SELECT MAX(`id`) FROM `dummy` '
        )

    def test_min(self):
        self.assertEqual(
            funcs.MIN(Dummy.id).get_sql_and_params()[0],
            'MIN(`id`)'
        )
        self.assertEqual(
            funcs.MIN(1, 2),
            min(1, 2)
        )

    def test_if(self):
        r = funcs.IF(Dummy.id > 2).THEN(1).ELSE(None)
        self.assertEqual(
            r.get_sql_and_params(),
            ('CASE WHEN `id` > %s THEN %s ELSE %s END', [2, 1, None])
        )
        r = r.alias('c')
        self.assertEqual(
            r.get_sql_and_params(),
            ('(CASE WHEN `id` > %s THEN %s ELSE %s END) AS c', [2, 1, None])
        )
