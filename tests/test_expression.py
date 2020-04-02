from olo.field import UnionField

from .base import Dummy, Foo, TestCase


class TestBinaryExpression(TestCase):
    def test_expression(self):
        cond = Dummy.id == Dummy.age
        ast = cond.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '=',
            ['COLUMN', 'dummy', 'id'],
            ['COLUMN', 'dummy', 'age']
        ])
        cond = Dummy.id > 2
        ast = cond.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '>',
            ['COLUMN', 'dummy', 'id'],
            ['VALUE', 2]
        ])
        cond = Dummy.id << [1, 2]
        ast = cond.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'IN',
            ['COLUMN', 'dummy', 'id'],
            ['VALUE', (1, 2)]
        ])
        cond = (Dummy.id > 1) & ((Dummy.age == 3) | (Dummy.age == 2))
        ast = cond.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'AND',
            ['BINARY_OPERATE',
                '>',
                ['COLUMN', 'dummy', 'id'],
                ['VALUE', 1]
             ],
            ['BINARY_OPERATE',
                'OR',
                ['BINARY_OPERATE',
                 '=',
                 ['COLUMN', 'dummy', 'age'],
                 ['VALUE', 3]],
                ['BINARY_OPERATE',
                 '=',
                 ['COLUMN', 'dummy', 'age'],
                 ['VALUE', 2]
                 ]]
        ])
        cond = (Dummy.id > 2) & (Dummy.age == 3) | (Dummy.age == 1)
        ast = cond.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'OR',
            ['BINARY_OPERATE',
             'AND',
                ['BINARY_OPERATE',
                 '>',
                 ['COLUMN', 'dummy', 'id'],
                 ['VALUE', 2]
                 ],
                ['BINARY_OPERATE',
                 '=',
                 ['COLUMN', 'dummy', 'age'],
                 ['VALUE', 3]]],
            ['BINARY_OPERATE',
                '=',
                ['COLUMN', 'dummy', 'age'],
                ['VALUE', 1]],
        ])
        cond = ((Dummy.age == 3) | (Dummy.age == 2)) & (Dummy.id > 2)
        ast = cond.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'AND',
            ['BINARY_OPERATE',
             'OR',
                ['BINARY_OPERATE',
                 '=',
                 ['COLUMN', 'dummy', 'age'],
                 ['VALUE', 3]
                 ],
                ['BINARY_OPERATE',
                 '=',
                 ['COLUMN', 'dummy', 'age'],
                 ['VALUE', 2]]],
            ['BINARY_OPERATE',
                '>',
                ['COLUMN', 'dummy', 'id'],
                ['VALUE', 2]],
        ])
        cond = (Dummy.id > 2) & ((Dummy.age == 3) | (Dummy.age == 2)) & (Dummy.id < 4)  # noqa
        ast = cond.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'AND',
            ['BINARY_OPERATE',
             'AND',
             ['BINARY_OPERATE',
              '>',
              ['COLUMN', 'dummy', 'id'],
              ['VALUE', 2]],
             ['BINARY_OPERATE',
              'OR',
              ['BINARY_OPERATE',
               '=',
               ['COLUMN', 'dummy', 'age'],
               ['VALUE', 3]
               ],
              ['BINARY_OPERATE',
               '=',
               ['COLUMN', 'dummy', 'age'],
               ['VALUE', 2]]],
             ],
            ['BINARY_OPERATE',
                '<',
                ['COLUMN', 'dummy', 'id'],
                ['VALUE', 4]],
        ])
        exp = Dummy.age == Dummy.age + 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '=',
            ['COLUMN', 'dummy', 'age'],
            ['BINARY_OPERATE',
             '+',
             ['COLUMN', 'dummy', 'age'],
             ['VALUE', 1]]
        ])
        exp = Dummy.age == 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '=',
            ['COLUMN', 'dummy', 'age'],
            ['VALUE', 1]
        ])
        exp = Dummy.age.in_([1, 2])
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'IN',
            ['COLUMN', 'dummy', 'age'],
            ['VALUE', (1, 2)]
        ])
        a = Dummy.age.alias('xixi')
        exp = a == 1
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            '=',
            ['ALIAS',
             ['COLUMN', 'dummy', 'age'],
             'xixi'],
            ['VALUE', 1]
        ])
        a = Dummy.age.asc()
        ast = a.get_sql_ast()
        self.assertEqual(ast, [
            'UNARY_OPERATE',
            ['COLUMN', 'dummy', 'age'],
            'ASC',
            True
        ])
        a = Dummy.age == 1
        exp = a.asc()
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'UNARY_OPERATE',
            ['BINARY_OPERATE',
             '=',
             ['COLUMN', 'dummy', 'age'],
             ['VALUE', 1]],
            'ASC',
            True
        ])
        f = UnionField(Dummy.age, Dummy.id)
        exp = f.not_in_([1])
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'NOT IN',
            ['BRACKET',
             ['COLUMN', 'dummy', 'age'],
             ['COLUMN', 'dummy', 'id']],
            ['VALUE', (1,)]
        ])
        exp = Dummy.id.in_(Foo.query('id').filter(Foo.age > 2))
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'IN',
            ['COLUMN', 'dummy', 'id'],
            ['SELECT',
             ['SERIES',
              ['COLUMN', 'foo', 'id']],
             ['FROM',
              ['TABLE', 'foo'],
              ],
             ['WHERE',
              ['BINARY_OPERATE',
               '>',
               ['COLUMN', 'foo', 'age'],
               ['VALUE', 2]]]]
        ])

    def test_const(self):
        exp0 = Dummy.id > 100
        exp = exp0 | 1
        ast = exp.get_sql_ast()
        self.assertEqual(
            ast,
            ['BINARY_OPERATE',
             'OR',
             ['BINARY_OPERATE',
              '>',
              ['COLUMN', 'dummy', 'id'],
              ['VALUE', 100]],
             ['BINARY_OPERATE',
              '!=',
              ['VALUE', 1],
              ['VALUE', 0]]
             ]
        )
        exp = exp0 | 'a'
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'OR',
            ['BINARY_OPERATE',
             '>',
             ['COLUMN', 'dummy', 'id'],
             ['VALUE', 100]],
            ['BINARY_OPERATE',
             '>',
             ['CALL', 'LENGTH', ['VALUE', 'a']],
             ['VALUE', 0]]
        ])
        exp = exp0 | True
        ast = exp.get_sql_ast()
        self.assertEqual(ast, [
            'BINARY_OPERATE',
            'OR',
            ['BINARY_OPERATE',
             '>',
             ['COLUMN', 'dummy', 'id'],
             ['VALUE', 100]],
            ['VALUE', 1]
        ])
        self.assertRaises(TypeError, lambda: exp0 | [])

    def test_optimize(self):
        exp = Dummy.age.in_([1, 2]) == False  # noqa
        self.assertEqual(
            exp.get_sql_ast(),
            ['BINARY_OPERATE',
             'NOT IN',
             ['COLUMN', 'dummy', 'age'],
             ['VALUE', (1, 2)]]
        )
        exp = Dummy.age.in_([1, 2]) == True  # noqa
        self.assertEqual(
            exp.get_sql_ast(),
            ['BINARY_OPERATE',
             'IN',
             ['COLUMN', 'dummy', 'age'],
             ['VALUE', (1, 2)]]
        )
        exp = Dummy.age.not_in_([1, 2]) == False  # noqa
        self.assertEqual(
            exp.get_sql_ast(),
            ['BINARY_OPERATE',
             'IN',
             ['COLUMN', 'dummy', 'age'],
             ['VALUE', (1, 2)]]
        )
        exp = (Dummy.age == 1) == False  # noqa
        self.assertEqual(
            exp.get_sql_ast(),
            ['BINARY_OPERATE',
             '!=',
             ['COLUMN', 'dummy', 'age'],
             ['VALUE', 1]]
        )
