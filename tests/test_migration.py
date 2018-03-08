# coding: utf-8

from olo.migration import migration_db_field
from .base import TestCase, Dummy


attrs = dict(
    name='foo',
    tags=['a', 'b', 'c'],
    password='password',
    payload={
        'abc': ['1', 2, 3],
        'def': [4, '5', 6]
    }
)


class TestMigration(TestCase):
    def test_migration_db_field(self):
        Dummy._options.db_field_version = 0
        _attrs = attrs.copy()
        _attrs['prop1'] = ['a', 'b']
        Dummy.create(**_attrs)
        _attrs['prop1'] = ['c', 'd']
        Dummy.create(**_attrs)
        _attrs['prop1'] = ['e', 'f']
        Dummy.create(**_attrs)
        Dummy._options.db_field_version = 1
        d = Dummy.get(1)
        self.assertIsNone(d.prop1)
        migration_db_field(Dummy, 0, 1)
        d = Dummy.get(1)
        self.assertEqual(d.prop1, ['a', 'b'])
        d = Dummy.get(2)
        self.assertEqual(d.prop1, ['c', 'd'])
        d = Dummy.get(3)
        self.assertEqual(d.prop1, ['e', 'f'])
        del d.prop1
        self.assertIsNone(d.prop1)
        migration_db_field(Dummy, 1, 0, delete=True)
        Dummy._options.db_field_version = 0
        d = Dummy.get(3)
        self.assertIsNone(d.prop1)
        Dummy._options.db_field_version = 1
