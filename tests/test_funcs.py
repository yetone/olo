import math

from olo import funcs
from olo.errors import SupportError

from .base import Bar, Dummy, TestCase


class TestFuncs(TestCase):
    def test_count(self):
        self.assertEqual(
            funcs.COUNT(Dummy.id).get_sql_ast(),
            ['CALL', 'COUNT', ['COLUMN', 'dummy', 'id']]
        )
        self.assertEqual(
            funcs.COUNT(1).get_sql_ast(),
            ['CALL', 'COUNT', ['VALUE', 1]]
        )
        self.assertEqual(
            funcs.COUNT(Dummy).get_sql_ast(),
            ['CALL', 'COUNT', ['VALUE', 1]]
        )
        self.assertEqual(
            funcs.COUNT(Dummy.id > 2).get_sql_ast(),
            ['CALL',
             'COUNT',
             ['IF',
              ['BINARY_OPERATE',
               '>',
               ['COLUMN', 'dummy', 'id'],
               ['VALUE', 2]],
              ['VALUE', 1],
              ['VALUE', None]]]
        )
        self.assertEqual(
            funcs.COUNT(Dummy.query).get_sql_ast(),
            ['SELECT',
             ['SERIES',
              ['CALL', 'COUNT', ['VALUE', 1]]],
             ['FROM',
              ['TABLE', 'dummy']]]
        )
        self.assertEqual(
            funcs.COUNT(Dummy.query.filter(id=1)).get_sql_ast(),
            ['SELECT',
             ['SERIES',
              ['CALL', 'COUNT', ['VALUE', 1]]],
             ['FROM',
              ['TABLE', 'dummy']],
             ['WHERE',
              ['BINARY_OPERATE',
               '=',
               ['COLUMN', 'dummy', 'id'],
               ['VALUE', 1]]]]
        )
        c = funcs.COUNT(1).alias('c')
        exp = c > 1
        self.assertEqual(
            exp.get_sql_ast(),
            ['BINARY_OPERATE',
             '>',
             ['ALIAS',
              ['CALL', 'COUNT', ['VALUE', 1]],
              'c'],
             ['VALUE', 1]]
        )
        exp = Dummy.id == c
        self.assertEqual(
            exp.get_sql_ast(),
            ['BINARY_OPERATE',
             '=',
             ['COLUMN', 'dummy', 'id'],
             ['ALIAS',
              ['CALL', 'COUNT', ['VALUE', 1]],
              'c']]
        )
        c = funcs.COUNT(1)
        exp = c > 1
        self.assertEqual(
            exp.get_sql_ast(),
            ['BINARY_OPERATE',
             '>',
             ['CALL', 'COUNT', ['VALUE', 1]],
             ['VALUE', 1]]
        )
        exp = Dummy.id == c
        self.assertEqual(
            exp.get_sql_ast(),
            ['BINARY_OPERATE',
             '=',
             ['COLUMN', 'dummy', 'id'],
             ['CALL', 'COUNT', ['VALUE', 1]]]
        )

    def test_sum(self):
        self.assertEqual(
            funcs.SUM(Dummy.id).get_sql_ast(),
            ['CALL',
             'SUM',
             ['COLUMN', 'dummy', 'id']]
        )
        self.assertEqual(
            funcs.SUM(Dummy.id > 2).get_sql_ast(),
            ['CALL',
             'SUM',
             ['BINARY_OPERATE',
              '>',
              ['COLUMN', 'dummy', 'id'],
              ['VALUE', 2]]]
        )

    def test_distinct(self):
        self.assertEqual(
            funcs.DISTINCT(Dummy.id).get_sql_ast(),
            ['CALL',
             'DISTINCT',
             ['COLUMN', 'dummy', 'id']]
        )

    def test_avg(self):
        self.assertEqual(
            funcs.AVG(Dummy.id).get_sql_ast(),
            ['CALL',
             'AVG',
             ['COLUMN', 'dummy', 'id']]
        )

    def test_sqrt(self):
        self.assertEqual(
            funcs.SQRT(Dummy.id).get_sql_ast(),
            ['CALL',
             'SQRT',
             ['COLUMN', 'dummy', 'id']]
        )
        self.assertEqual(
            funcs.SQRT(2),
            math.sqrt(2)
        )

    def test_max(self):
        self.assertEqual(
            funcs.MAX(Dummy.id).get_sql_ast(),
            ['CALL',
             'MAX',
             ['COLUMN', 'dummy', 'id']]
        )
        self.assertEqual(
            funcs.MAX(1, 2),
            max(1, 2)
        )
        self.assertRaises(SupportError, lambda: funcs.MAX(Dummy))
        self.assertRaises(SupportError, lambda: funcs.MAX(Dummy.query))
        self.assertRaises(SupportError, lambda: funcs.MAX(Dummy.cq))
        # self.assertRaises(SupportError, lambda: funcs.MAX(Dummy.query('id')))
        self.assertEqual(
            funcs.MAX(Dummy.query('id')).get_sql_ast(),
            ['SELECT',
             ['SERIES',
              ['CALL',
               'MAX',
               ['COLUMN', 'dummy', 'id']]],
             ['FROM',
              ['TABLE', 'dummy'],
              ]]
        )

    def test_min(self):
        self.assertEqual(
            funcs.MIN(Dummy.id).get_sql_ast(),
            ['CALL',
             'MIN',
             ['COLUMN', 'dummy', 'id']]
        )
        self.assertEqual(
            funcs.MIN(1, 2),
            min(1, 2)
        )

    def test_if(self):
        r = funcs.IF(Dummy.id > 2).THEN(1).ELSE(None)
        self.assertEqual(
            r.get_sql_ast(),
            ['IF',
             ['BINARY_OPERATE',
              '>',
              ['COLUMN', 'dummy', 'id'],
              ['VALUE', 2]],
             ['VALUE', 1],
             ['VALUE', None]]
        )
        r = r.alias('c')
        self.assertEqual(
            r.get_sql_ast(),
            ['ALIAS',
             ['IF',
              ['BINARY_OPERATE',
               '>',
               ['COLUMN', 'dummy', 'id'],
               ['VALUE', 2]],
              ['VALUE', 1],
              ['VALUE', None]],
             'c'
             ]
        )
