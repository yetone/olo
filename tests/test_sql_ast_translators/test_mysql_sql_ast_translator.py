from olo.sql_ast_translators.mysql_sql_ast_translator import MySQLSQLASTTranslator  # noqa

from tests.base import TestCase


tran = MySQLSQLASTTranslator()


class TestMySQLSQLASTTranslator(TestCase):
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
