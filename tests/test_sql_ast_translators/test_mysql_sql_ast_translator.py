from olo.sql_ast_translators.mysql_sql_ast_translator import MySQLSQLASTTranslator  # noqa
from tests.base import TestCase

tran = MySQLSQLASTTranslator()


class TestMySQLSQLASTTranslator(TestCase):
    def test_select(self):
        ast = ['SELECT',
               ['SERIES',
                ['COLUMN', 'b', 'name'],
                ['COLUMN', 'b', 'age'],
                ['COLUMN', 'f', 'age']],
               ['FROM',
                ['JOIN',
                 'INNER',
                 ['ALIAS',
                  ['TABLE', 'bar'],
                  'b'],
                 ['ALIAS',
                  ['TABLE', 'foo'],
                  'f'],
                 ['BINARY_OPERATE',
                  '=',
                  ['COLUMN', 'b', 'age'],
                  ['COLUMN', 'f', 'age']]]],
               ['WHERE',
                ['BINARY_OPERATE',
                 '<',
                 ['COLUMN', 'b', 'age'],
                 ['SELECT',
                  ['SERIES',
                   ['CALL',
                    'MAX',
                    ['COLUMN', 'f', 'id']]],
                  ['FROM',
                   ['ALIAS',
                    ['TABLE', 'foo'],
                    'f']]]]],
               ['GROUP BY',
                ['SERIES',
                 ['COLUMN', 'b', 'age']]],
               ['ORDER BY',
                ['SERIES',
                 ['UNARY_OPERATE',
                  ['COLUMN', 'b', 'age'],
                  'DESC']]],
               ['LIMIT',
                ['VALUE', 20],
                ['VALUE', 10]]]
        self.assertEqual(
            tran.translate(ast),
            ('SELECT `b`.`name`, `b`.`age`, `f`.`age` FROM `bar` AS b INNER JOIN `foo` AS f ON `b`.`age` = `f`.`age` WHERE `b`.`age` < (SELECT MAX(`f`.`id`) FROM `foo` AS f) GROUP BY `b`.`age` ORDER BY `b`.`age` DESC LIMIT %s, %s',  # noqa
             [20, 10])
        )

    def test_or(self):
        self.assertEqual(
            tran.translate([
                'OR',
                ['BINARY_OPERATE',
                 '>',
                 ['COLUMN', 'dummy', 'age'],
                 ['VALUE', 2]],
                ['BINARY_OPERATE',
                 '>',
                 ['COLUMN', 'dummy', 'id'],
                 ['VALUE', 3]],
                ['BINARY_OPERATE',
                 '<',
                 ['COLUMN', 'dummy', 'id'],
                 ['VALUE', 12]],
            ]),
            (
                '`dummy`.`age` > %s OR `dummy`.`id` > %s OR `dummy`.`id` < %s',
                [2, 3, 12]
            )
        )

    def test_if(self):
        self.assertEqual(
            tran.translate([
                'IF',
                ['BINARY_OPERATE',
                 '>',
                 ['COLUMN', 'dummy', 'age'],
                 ['VALUE', 2]],
                ['VALUE', 'a'],
                ['VALUE', 'b']
            ]),
            (
                'CASE WHEN `dummy`.`age` > %s THEN %s ELSE %s END',
                [2, 'a', 'b']
            )
        )

    def test_for_update(self):
        self.assertEqual(
            tran.translate([
                'SELECT',
                ['SERIES',
                 ['COLUMN', 'f', 'age']],
                ['FROM',
                 ['ALIAS',
                  ['TABLE', 'foo'],
                  'f']],
                ['WHERE',
                 ['BINARY_OPERATE',
                  '>',
                  ['COLUMN', 'f', 'age'],
                  ['VALUE', 2]],
                 ],
                ['FOR UPDATE']
            ]),
            (
                'SELECT `f`.`age` FROM `foo` AS f WHERE `f`.`age` > %s FOR UPDATE',
                [2]
            )
        )
