# coding: utf-8

import pickle

from datetime import datetime, date
from enum import Enum

from olo.compat import PY2, Decimal, unicode
from olo.utils import (
    type_checker, transform_type, Missing, ThreadedObject,
    friendly_repr,
)
from .base import TestCase


class A(object):
    name = 'A'


class AEnum(Enum):
    A = 0
    B = 1
    C = 2


class TestUtils(TestCase):
    def test_type_checker(self):
        self.assertTrue(type_checker(int, 1))
        self.assertTrue(type_checker(str, '1'))
        self.assertTrue(type_checker(list, ['a']))
        self.assertTrue(type_checker(dict, {'a': 1}))
        self.assertTrue(type_checker({}, {'a': 1}))
        self.assertTrue(type_checker(datetime, datetime.now()))
        self.assertFalse(type_checker(int, '1'))
        self.assertFalse(type_checker(str, 1))
        self.assertFalse(type_checker(list, {'a': 1}))
        self.assertFalse(type_checker(dict, [1]))
        self.assertFalse(type_checker(datetime, 1))
        self.assertTrue(type_checker([], [1, 2, 3]))
        self.assertTrue(type_checker([int], [1, 2, 3]))
        self.assertTrue(type_checker({int: str}, {1: '1', 2: '2', 3: '3'}))
        self.assertTrue(type_checker({int: [int]}, {1: [1, 2, 3], 2: [3, 4, 5],
                                                    3: [5, 6, 7, 8, 9]}))
        self.assertFalse(type_checker([int], [1, 2, 3, '4']))
        self.assertFalse(type_checker({int: str}, {1: '1', 2: '2', 3: 3}))
        self.assertFalse(type_checker({int: [int]}, {1: [1, 2, 3],
                                                     2: [3, 4, 5],
                                                     3: [5, 6, 7, 8, '9']}))
        self.assertFalse(type_checker({str: int}, {'a': 1, 'b': '2'}))
        self.assertTrue(type_checker((int,), (1,)))
        self.assertFalse(type_checker((int,), (1, 2)))
        self.assertTrue(type_checker((int, str), (1, "a")))
        self.assertTrue(type_checker((int, (str,)), (1, ("a",))))
        self.assertFalse(type_checker((int, (str,)), (1, ("a", "b"))))
        # Just make coverage happing!!!
        self.assertFalse(type_checker({int}, {1}))

    def test_transform_type(self):
        self.assertEqual(transform_type('管', unicode), u'管')
        self.assertEqual(transform_type(u'管', str), '管')
        self.assertEqual(transform_type(u'1', str), '1')
        self.assertEqual(transform_type([1], str), '[1]')
        self.assertEqual(transform_type({1: 2}, str), '{"1": 2}')
        self.assertEqual(transform_type({'1': '1', u'3': '管'}, {int: unicode}),
                         {1: u'1', 3: u'管'})
        self.assertEqual(transform_type('[1,2,3,"4"]', [int]), [1, 2, 3, 4])
        self.assertEqual(transform_type('[1,2,3,"4"]', [float]), [1, 2, 3, 4])
        self.assertEqual(transform_type('[1,2,3,"4"]', list), [1, 2, 3, "4"])
        self.assertEqual(transform_type('{"a": [1,2,3]}', dict),
                         {'a': [1, 2, 3]})
        self.assertEqual(transform_type(1.23, Decimal), Decimal('1.23'))
        self.assertEqual(transform_type('1.23', Decimal), Decimal('1.23'))
        self.assertEqual(transform_type('{"a": ["管"]}', {str: [unicode]}),
                         {'a': [u'管']})
        self.assertEqual(transform_type('[["2015-12-12 08:23:01"]]',
                                        [[datetime]]),
                         [[datetime(2015, 12, 12, 8, 23, 1)]])
        self.assertEqual(transform_type(0, bool), False)
        self.assertEqual(transform_type(1, bool), True)
        self.assertEqual(transform_type(3.14, Decimal), Decimal('3.14'))
        self.assertEqual(transform_type('8.88', Decimal), Decimal('8.88'))
        self.assertEqual(transform_type([u'123', '456'], [str]),
                         ['123', '456'])
        self.assertEqual(transform_type('(1, 2, 3)', tuple), (1, 2, 3))
        self.assertEqual(transform_type((1, 2, 3), str), '(1, 2, 3)')
        self.assertEqual(transform_type(1, unicode), u'1')
        self.assertEqual(transform_type([('a', 1)], dict), {'a': 1})
        self.assertEqual(transform_type('2017-05-27', date),
                         datetime(2017, 5, 27).date())
        self.assertEqual(transform_type([1, 2], tuple), (1, 2))
        with self.assertRaises(TypeError):
            transform_type('[1]', {})
        self.assertEqual(transform_type("1", "string"), "1")
        self.assertEqual(transform_type('A', AEnum), AEnum.A)
        self.assertEqual(transform_type(AEnum.B, str), 'B')

    def test_missing(self):
        self.assertTrue(Missing() == Missing())
        self.assertFalse(Missing() != Missing())
        self.assertFalse(bool(Missing()))

    def test_treaded_object(self):
        a = ThreadedObject(A)
        self.assertEqual(a.name, 'A')
        v = pickle.dumps(a)
        _a = pickle.loads(v)
        self.assertEqual(_a.name, 'A')

    def test_friendly_repr(self):
        self.assertEqual(friendly_repr(1), '1')
        if PY2:
            self.assertEqual(friendly_repr('aaa'), "b'aaa'")
            self.assertEqual(friendly_repr(u'aaa'), "u'aaa'")
        else:
            self.assertEqual(friendly_repr('aaa'), "'aaa'")
