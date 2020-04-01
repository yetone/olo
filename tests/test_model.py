# coding=utf-8
import json
import pickle
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

from mock import patch, Mock

from olo import Field, DbField, Model
from olo.key import StrKey
from olo.libs.aes import encrypt
from olo.utils import transform_type, missing, override
from olo.errors import (
    ValidationError, ParseError, InvalidFieldError
)
from olo.migration import MigrationVersion
from olo.compat import PY2, str_types, xrange, to_str
from .base import db, TestCase, BaseModel, Dummy, Bar, db, Ttt, Foo, Lala
from .utils import auto_use_cache_ctx, patched_execute


attrs = dict(
    name='foo',
    tags=['a', 'b', 'c'],
    password='password',
    payload={
        'abc': ['1', 2, 3],
        'def': [4, '5', 6]
    }
)


class _Dummy(Dummy):
    class Options:
        foo = 'bar'

    name = Field(int)

    @classmethod
    def validate_name(cls, name):
        if name > 1000:
            raise ValidationError('%s more than 1000' % name)


db.create_all()


class TestModel(TestCase):
    def test_keys(self):
        self.assertTrue(StrKey(['id']) not in Dummy.__index_keys__)
        # self.assertTrue(StrKey(['name']) in Dummy.__index_keys__)
        # self.assertTrue(StrKey(['name', 'age']) in Dummy.__index_keys__)
        # self.assertTrue(StrKey(['name', 'age']) not in Dummy.__unique_keys__)
        self.assertTrue(StrKey(['id']) not in Lala.__unique_keys__)
        # self.assertTrue(StrKey(['name', 'age']) in Lala.__unique_keys__)

    def test_override(self):
        with self.assertRaises(RuntimeError):
            class A(Model):
                def _clone(self):
                    pass

        class B(Model):
            @override
            def _clone(self):
                pass

    def test_create(self):
        dummy = Dummy.create(**attrs)
        self.assertEqual(dummy.id, 1)
        self.assertEqual(dummy.dynasty, '现代')
        dummy = Dummy.create(**attrs)
        self.assertEqual(dummy.id, 2)
        dummy = Dummy.create(**attrs)
        self.assertEqual(dummy.id, 3)
        # except error
        with self.assertRaises(InvalidFieldError):
            _attrs = dict(n=1, **attrs)
            Dummy.create(**_attrs)
        _attrs = dict(age='a', **attrs)
        with self.assertRaises(ParseError):
            Dummy.create(**_attrs)
        dummy = Dummy.create(**dict(dynasty=None, **attrs))
        self.assertEqual(dummy.dynasty, '现代')
        dummy = Dummy.create(**dict(dynasty1=None, **attrs))
        self.assertEqual(dummy.dynasty1, None)
        bc = Dummy.before_create
        try:
            Dummy.before_create = classmethod(lambda cls, **kwargs: False)
            dummy = Dummy.create(name='test')
            self.assertIsNone(dummy)
        finally:
            Dummy.before_create = bc
        dummy = Dummy.get_by(name='test')
        self.assertIsNone(dummy)
        d = Dummy.create(name='dummy', prop1=[1])
        self.assertEqual(d.name, 'dummy')
        self.assertEqual(d.prop1, ['1'])
        d = Dummy.get(d.id)
        self.assertEqual(d.name, 'dummy')
        self.assertEqual(d.prop1, ['1'])
        old_after_create = Dummy.after_create
        try:
            Dummy.after_create = (
                lambda self, *args:
                self.update(name=self.name + 'b')
            )
            d = Dummy.create(name='a', prop1=[1])
            self.assertEqual(d.name, 'ab')
        finally:
            Dummy.after_create = old_after_create

    def test_save(self):
        dummy = Dummy(**attrs)
        self.assertEqual(dummy.name, attrs['name'])
        self.assertIsNone(dummy.id)
        dummy.save()
        self.assertIsNotNone(dummy.id)
        self.assertEqual(dummy.name, attrs['name'])

    def test_extend_missing_data(self):
        dummy = Dummy.create(name='1')
        dummy._extend_missing_data()
        self.assertEqual(dummy.flag, 0)

    def test_count_by(self):
        Foo.create(name='foo', age=1)
        Foo.create(name='bar', age=1)
        Foo.create(name='bar', age=2)
        c = Foo.count_by(age=1)
        self.assertEqual(c, 2)
        with auto_use_cache_ctx(Foo):
            with patched_execute as execute:
                c = Foo.count_by(age=1)
                self.assertEqual(c, 2)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                c = Foo.count_by(age=1)
                self.assertEqual(c, 2)
                self.assertFalse(execute.called)

    def test_get_by(self):
        Dummy.create(name='foo')
        Dummy.create(name='bar')
        dummy = Dummy.get_by(name='bar')
        self.assertEqual(dummy.name, 'bar')
        # except error
        with self.assertRaises(InvalidFieldError):
            Dummy.get_by(name='bar', x=1)
        Foo.create(name='xixi', age=1)
        Foo.create(name='haha', age=2)
        with auto_use_cache_ctx(Foo):
            with patched_execute as execute:
                foo = Foo.get_by(name='haha', age=2)
                self.assertEqual(foo.name, 'haha')
                self.assertTrue(execute.called)
            with patched_execute as execute:
                foo = Foo.get_by(name='haha', age=2)
                self.assertEqual(foo.name, 'haha')
                self.assertFalse(execute.called)

    def test_gets_by(self):
        Dummy.create(name='foo', age=2)
        Dummy.create(name='foo', age=1)
        Dummy.create(name='bar', age=4)
        Dummy.create(name='bar', age=5)
        Dummy.create(name='bar', age=6)
        Dummy.create(name='bar', age=3)
        dummys = Dummy.gets_by(name='foo')
        self.assertEqual(len(dummys), 2)
        dummys = Dummy.gets_by(order_by=Dummy.age.desc())
        self.assertEqual(dummys[0].age, 6)
        dummys = Dummy.gets_by(order_by=Dummy.age)
        self.assertEqual(dummys[0].age, 1)

    def test_get(self):
        dummy = Dummy.create(**attrs)
        self.assertEqual(Dummy.get(dummy.id).name, attrs['name'])
        self.assertEqual(Dummy.get(name=dummy.name).id, dummy.id)
        self.assertNotEqual(Dummy.get(dummy.id).payload, attrs['payload'])
        self.assertEqual(Dummy.get(dummy.id).payload,
                         transform_type(attrs['payload'], Dummy.payload.type))
        self.assertEqual(Dummy.get(dummy.id).created_date, date.today())
        self.assertTrue(isinstance(Dummy.get(dummy.id).__primary_key__, tuple))
        Bar.create(name='1', age=2)
        self.assertEqual(Bar.get('1').age, 2)
        with auto_use_cache_ctx(Dummy):
            Dummy.create(id=233, **attrs)
            with patched_execute as execute:
                _dummy = Dummy.get(233)
                self.assertIsNotNone(_dummy)
                self.assertEqual(_dummy.id, 233)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                _dummy = Dummy.get(233)
                self.assertIsNotNone(_dummy)
                self.assertEqual(_dummy.id, 233)
                self.assertFalse(execute.called)

    def test_gets(self):
        Dummy.create(name='foo', age=2)
        Dummy.create(name='foo', age=1)
        Dummy.create(name='bar', age=4)
        Dummy.create(name='bar', age=5)
        Dummy.create(name='bar', age=6)
        Dummy.create(name='bar', age=3)
        ids = [1, 3, 4]
        dummys = Dummy.gets(ids)
        self.assertEqual(len(dummys), 3)
        self.assertEqual(list(map(lambda x: x.id, dummys)), ids)
        _ids = [1, 300, 4, 100, 2]
        self.assertEqual(len(Dummy.gets(_ids)), 3)
        dummys = Dummy.gets(_ids, filter_none=False)
        self.assertEqual(len(dummys), len(_ids))
        self.assertEqual(dummys[1], None)
        self.assertEqual(dummys[3], None)
        dummys = Dummy.gets([
            {'name': 'bar', 'age': 6},
            {'name': 'foo', 'age': 1},
            {'name': 'bar', 'age': 3},
            {'name': 'foo', 'age': 8},
        ], filter_none=False)
        self.assertEqual(len(dummys), 4)
        self.assertEqual(dummys[0].age, 6)
        self.assertEqual(dummys[-1], None)
        Bar.create(name='1', age=2)
        Bar.create(name='2', age=2)
        Bar.create(name='3', age=2)
        self.assertEqual(len(Bar.gets(['1', '2', '3'])), 3)
        with auto_use_cache_ctx(Dummy, Bar):
            with patched_execute as execute:
                dummys = Dummy.gets(ids)
                self.assertEqual(len(dummys), 3)
                self.assertEqual(list(map(lambda x: x.id, dummys)), ids)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                dummys = Dummy.gets(ids)
                self.assertEqual(len(dummys), 3)
                self.assertEqual(list(map(lambda x: x.id, dummys)), ids)
                self.assertFalse(execute.called)
            with patched_execute as execute:
                self.assertEqual(len(Bar.gets(['1', '2', '3'])), 3)
                self.assertTrue(execute.called)
            with patched_execute as execute:
                self.assertEqual(len(Bar.gets(['1', '2', '3'])), 3)
                self.assertFalse(execute.called)

        #  test str id
        Ttt.create()
        Ttt.create()
        ts = Ttt.gets([1, 2])
        self.assertEqual(len(ts), 2)

    def test_update(self):
        dummy = Dummy.create(**attrs)
        self.assertEqual(dummy.name, 'foo')
        dummy.name = 'bar'
        self.assertEqual(dummy.name, 'bar')
        dummy.save()
        dummy = Dummy.query.filter(id=dummy.id).first()
        self.assertEqual(dummy.name, 'bar')
        payload = {
            'xxx': ['1', 2, 3],
            'yyy': [4, '5', 6]
        }
        self.assertFalse(dummy.is_dirty())
        dummy.payload = payload
        self.assertTrue(dummy.is_dirty())
        dummy.save()
        self.assertFalse(dummy.is_dirty())
        dummy = Dummy.query.filter(id=dummy.id).first()
        self.assertEqual(dummy.payload, transform_type(payload,
                                                       Dummy.payload.type))
        dummy.payload = json.dumps(payload)
        dummy.save()
        dummy = Dummy.query.filter(id=dummy.id).first()
        self.assertEqual(dummy.payload, transform_type(payload,
                                                       Dummy.payload.type))
        dt = datetime.now()
        dummy.update(db_dt=dt)
        self.assertEqual(dummy.db_dt, dt)
        dummy.update(db_dt=None)
        self.assertIsNone(dummy.db_dt)
        dummy = Dummy.get(dummy.id)
        self.assertIsNone(dummy.db_dt)
        dummy.update(db_dt=dt)
        self.assertEqual(dummy.db_dt, dt)
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.db_dt, dt)
        r = dummy.update()
        self.assertFalse(r)
        bu = dummy.before_update
        name = dummy.name
        try:
            dummy.before_update = lambda **kwargs: False
            r = dummy.update(name='xixixixixixxi')
        finally:
            dummy.before_update = bu
        self.assertFalse(r)
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.name, name)
        dummy.update(prop1=[1, 2, 3])
        self.assertEqual(dummy.prop1, ['1', '2', '3'])
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, ['1', '2', '3'])

    def test_delete(self):
        dummy = Dummy.create(**attrs)
        dummy1 = Dummy.create(**attrs)
        dummy2 = Dummy.create(**attrs)
        dummy.delete()
        dummy = Dummy.query.filter(id=dummy.id).first()
        self.assertTrue(dummy is None)
        dummy = Dummy.query.filter(id=dummy1.id).first()
        self.assertTrue(dummy is not None)
        dummy = Dummy.query.filter(id=dummy2.id).first()
        self.assertTrue(dummy is not None)
        bd = dummy.before_delete
        try:
            dummy.before_delete = lambda **kwargs: False
            dummy.delete()
        finally:
            dummy.before_delete = bd
        dummy = Dummy.query.filter(id=dummy.id).first()
        self.assertTrue(dummy is not None)

    def test_on_update(self):
        dummy = Dummy.create(**attrs)
        old_age = dummy.age
        old_count = dummy.count
        self.assertEqual(dummy.name, 'foo')
        dummy.name = 'bar'
        dummy.save()
        self.assertEqual(dummy.age, old_age + 1)
        self.assertEqual(dummy.count, old_count + 3)
        dummy = Dummy.query.filter(id=dummy.id).first()
        self.assertEqual(dummy.age, old_age + 1)
        self.assertEqual(dummy.count, old_count + 3)

    def test_will_update(self):
        def will_update(_self, next_inst):
            self.assertEqual(_self.id, next_inst.id)
            self.assertEqual(_self.name, 'foo')
            self.assertEqual(_self.age, 1)
            self.assertEqual(next_inst.name, 'bar')
            self.assertEqual(next_inst.age, 2)
            return True
        Dummy.will_update = will_update
        dummy = Dummy.create(name='foo', age=1)
        dummy.name = 'bar'
        dummy.age = 2
        is_success = dummy.save()
        self.assertTrue(is_success)
        self.assertEqual(dummy.name, 'bar')
        self.assertEqual(dummy.age, 2)

        def will_update(_self, next_inst):
            self.assertEqual(_self.id, next_inst.id)
            self.assertEqual(_self.name, 'bar')
            self.assertEqual(_self.age, 2)
            self.assertEqual(next_inst.name, 'xixi')
            self.assertEqual(next_inst.age, 3)
            return True

        Dummy.will_update = will_update
        Dummy.name_will_update = Mock()
        Dummy.age_will_update = Mock()
        is_success = dummy.update(name='xixi', age=3)
        self.assertTrue(is_success)
        self.assertEqual(dummy.name, 'xixi')
        self.assertEqual(dummy.age, 3)
        self.assertTrue(Dummy.name_will_update.called)
        self.assertTrue(Dummy.age_will_update.called)

        def will_update(_self, next_inst):
            return False
        Dummy.will_update = will_update
        dummy.name = 'heheda'
        is_success = dummy.save()
        self.assertFalse(is_success)
        self.assertEqual(dummy.name, 'xixi')

        def will_update(_self, next_inst):
            return True

        def age_will_update(_self, next_age):
            self.assertEqual(next_age, 4)
            self.assertEqual(_self.age, 3)
            return False

        Dummy.will_update = will_update
        Dummy.name_will_update = Mock()
        Dummy.age_will_update = age_will_update
        Dummy.count_will_update = Mock()
        is_success = dummy.update(age=4)
        self.assertFalse(is_success)
        self.assertEqual(dummy.age, 3)
        self.assertFalse(Dummy.name_will_update.called)
        # Dummy.count.on_update but age_will_update return False
        # self.assertFalse(Dummy.count_will_update.called)

    def test_did_update(self):
        def did_update(_self, orig):
            self.assertEqual(_self.id, orig.id)
            self.assertEqual(orig.name, 'foo')
            self.assertEqual(orig.age, 1)
            self.assertEqual(_self.name, 'bar')
            self.assertEqual(_self.age, 2)
        Dummy.did_update = did_update
        dummy = Dummy.create(name='foo', age=1)
        dummy.name = 'bar'
        dummy.age = 2
        dummy.save()

        def did_update(_self, orig):
            self.assertEqual(_self.id, orig.id)
            self.assertEqual(orig.name, 'bar')
            self.assertEqual(orig.age, 2)
            self.assertEqual(_self.name, 'xixi')
            self.assertEqual(_self.age, 3)

        Dummy.did_update = did_update
        Dummy.name_did_update = Mock()
        Dummy.age_did_update = Mock()
        dummy.update(name='xixi', age=3)
        self.assertTrue(Dummy.name_did_update.called)
        self.assertTrue(Dummy.age_did_update.called)

        def did_update(_self, orig):
            pass

        def age_did_update(_self, orig_age):
            self.assertEqual(_self.age, 4)
            self.assertEqual(orig_age, 3)

        Dummy.did_update = did_update
        Dummy.name_did_update = Mock()
        Dummy.age_did_update = age_did_update
        Dummy.count1_did_update = Mock()
        dummy.update(age=4)
        self.assertFalse(Dummy.name_did_update.called)
        self.assertFalse(Dummy.count1_did_update.called)
        Dummy.count1_did_update = Mock()
        dummy.count1 = 9
        self.assertFalse(Dummy.count1_did_update.called)
        Dummy.age_did_update = Mock()
        Dummy.count1_did_update = Mock()
        dummy.update(count1=10)
        # 支持 DbField
        self.assertTrue(Dummy.count1_did_update.called)
        Dummy.count1_did_update = Mock()
        dummy.update(count1=dummy.count1)
        self.assertFalse(Dummy.count1_did_update.called)
        Dummy.age_did_update = Mock()
        dummy.update(age=dummy.age, count=0)
        self.assertFalse(Dummy.age_did_update.called)

        def age_did_update(_self, old_age):
            if old_age != 1:
                _self.update_age()

        def update_age(_self):
            _self.update(age=1, count=0)

        Dummy.update_age = update_age
        Dummy.age_did_update = age_did_update
        dummy.update_age()
        Dummy.age_did_update = Mock()

    def test_attr_did_update_with_transaction(self):
        Dummy.name_did_update = Mock()
        Dummy.age_did_update = Mock()

        with db.transaction():
            dummy = Dummy.create(name='foo', age=1)

            dummy.update(name='bar')
            dummy.update(age=2)

        self.assertEqual(
            (Dummy.name_did_update.called, Dummy.age_did_update.called),
            (True, True)
        )

    def test_after_update(self):
        dummy = Dummy.create(**attrs)
        with patch('tests.base.after_update') as after_update:
            dummy.name = 'bar'
            dummy.save()
            self.assertTrue(after_update.called)
        with patch('tests.base.after_update') as after_update:
            dummy.payload = {}
            dummy.save()
            self.assertTrue(after_update.called)
        with patch('tests.base.after_update') as after_update:
            dummy.update(prop1=['a'])
            dummy.save()
            self.assertTrue(after_update.called)
        with patch('tests.base.after_update') as after_update:
            dummy.prop1 = ['b']
            dummy.save()
            self.assertTrue(after_update.called)
        with patch('tests.base.after_update') as after_update:
            with db.transaction():
                dummy.name = 'bar0'
                dummy.save()
            self.assertTrue(after_update.called)
        with patch('tests.base.after_update') as after_update:
            try:
                with db.transaction():
                    dummy.name = 'bar0'
                    dummy.save()
                    raise Exception
            except Exception:
                pass
            self.assertFalse(after_update.called)

    def test_after_create(self):
        with patch('tests.base.after_create') as after_create:
            Dummy.create(**attrs)
            self.assertTrue(after_create.called)
        with patch('tests.base.after_create') as after_create:
            with db.transaction():
                Dummy.create(**attrs)
            self.assertTrue(after_create.called)
        with patch('tests.base.after_create') as after_create:
            try:
                with db.transaction():
                    Dummy.create(**attrs)
                    raise Exception
            except Exception:
                pass
            self.assertFalse(after_create.called)

    def test_inherit(self):
        _attrs = attrs.copy()
        _attrs['name'] = 233
        dummy = _Dummy.create(**_attrs)
        self.assertEqual(dummy.id, 1)
        self.assertEqual(dummy.name, 233)
        self.assertEqual(dummy.tags, ['a', 'b', 'c'])
        dummy.update(name=666)
        self.assertEqual(dummy.name, 666)
        self.assertTrue(isinstance(_Dummy.get(1).__primary_key__, tuple))
        self.assertTrue(_Dummy._options.db is Dummy._options.db)
        self.assertFalse(hasattr(Dummy._options, 'foo'))
        self.assertEqual(_Dummy._options.foo, 'bar')
        self.assertEqual(_Dummy._options.reason, 'test inherit')
        self.assertTrue(_Dummy._options.enable_log)

    def test_to_dict(self):
        dummy = Dummy.create(**attrs)
        dct = dummy.to_dict()
        self.assertTrue(isinstance(dct['created_at'], datetime))
        dct = dummy.to_dict(jsonize=True)
        self.assertTrue(isinstance(dct['created_at'], str_types))
        self.assertEqual(dct['name'], attrs['name'])
        dct = dummy.to_dict(excludes=['created_at'])
        self.assertTrue('created_at' not in dct)

    def test_choices(self):
        _attrs = attrs.copy()
        _attrs['flag'] = 3
        with self.assertRaises(ValidationError):
            Dummy.create(**_attrs)
        _attrs['name'] = 233
        with self.assertRaises(ValidationError):
            _Dummy.create(**_attrs)
        _attrs['flag'] = 2
        dummy = Dummy.create(**_attrs)
        dummy.flag = 0
        dummy.save()
        dummy.flag = '1'
        dummy.save()
        self.assertEqual(dummy.flag, 1)

    def test_encrypt(self):
        dummy = Dummy.create(**attrs)
        self.assertEqual(dummy.password, attrs['password'])
        self.assertEqual(Dummy.get(dummy.id).password, attrs['password'])
        self.assertEqual(Dummy.gets([dummy.id])[0].password, attrs['password'])
        not_raw = Dummy.query('password').filter(id=dummy.id).one()
        self.assertEqual(not_raw, attrs['password'])
        raw = Dummy.query('password', raw=True).filter(id=dummy.id).one()
        self.assertEqual(raw, to_str(encrypt(attrs['password'], Dummy.AES_KEY)))
        dummy.update(password='123')
        self.assertEqual(dummy.password, '123')
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.password, '123')
        raw = Dummy.query('password', raw=True).filter(id=dummy.id).one()
        self.assertEqual(raw, to_str(encrypt('123', Dummy.AES_KEY)))
        dummy.password = '456'
        dummy.save()
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.password, '456')
        old_password = Dummy.password
        try:
            Dummy.password = Field(
                str,
                name='password',
                input=lambda x: 'encrypted',
                output=lambda x: 'decrypted'
            )
            dummy = Dummy.create(**attrs)
            self.assertEqual(dummy.password, 'decrypted')
            self.assertEqual(
                Dummy.query('password', raw=True).filter(id=dummy.id).one(),
                'encrypted'
            )
        finally:
            Dummy.password = old_password

    def test_pickle(self):
        from dill import dumps, loads
        dummy = Dummy.create(**attrs)
        d = dumps(dummy, -1)
        self.assertEqual(dummy.password, pickle.loads(d).password)
        dummy = Dummy.get(dummy.id)
        d = dumps(dummy, -1)
        self.assertEqual(dummy.password, pickle.loads(d).password)
        _dummy = Dummy.cache.get(dummy.id)
        _dummy = Dummy.cache.get(dummy.id)
        self.assertEqual(dummy.password, _dummy.password)
        dumps(Dummy.password)
        f = Field(
            str,
            input=lambda x: 'encrypted',
            output=lambda x: 'decrypted'
        )
        dumps(f)
        dumps(Dummy.created_at)

    def test_db_field_v0(self):
        Dummy._options.db_field_version = 0
        _attrs = attrs.copy()
        _attrs['prop1'] = ['a', 'b', 'c']
        dummy = Dummy.create(**_attrs)
        self.assertEqual(dummy.count, 0)
        self.assertEqual(dummy.prop1, _attrs['prop1'])
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, _attrs['prop1'])
        dummy.prop1 = ['q', 'w', 'e']
        self.assertEqual(dummy.prop1, ['q', 'w', 'e'])
        dct = dummy.to_dict()
        self.assertEqual(dct['prop1'], ['q', 'w', 'e'])
        del dummy.prop1
        dummy = Dummy.get(dummy.id)
        self.assertIsNone(dummy.prop1)
        dummy.update(prop1=['U', 'I'])
        self.assertEqual(dummy.prop1, ['U', 'I'])
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, ['U', 'I'])
        dummy.prop1 = json.dumps(['a', 'b'])
        dummy.save()
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, ['a', 'b'])
        del dummy.prop1
        self.assertTrue(dummy.prop1 is None)

    def test_db_field_v1(self):
        Dummy._options.db_field_version = 1
        _attrs = attrs.copy()
        _attrs['prop1'] = ['a', 'b', 'c']
        dummy = Dummy.create(**_attrs)
        self.assertEqual(dummy.count, 0)
        self.assertEqual(dummy.prop1, _attrs['prop1'])
        del dummy.prop1
        self.assertTrue(dummy.prop1 is None)
        dummy.update(prop1=['e', 'f'])
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, ['e', 'f'])
        Dummy._options.db_field_version = 0
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, None)

    def test_db_field_migration_version(self):
        Dummy._options.db_field_version = 0
        _attrs = attrs.copy()
        _attrs['prop1'] = ['a', 'b', 'c']
        dummy = Dummy.create(**_attrs)
        self.assertEqual(dummy.prop1, _attrs['prop1'])
        Dummy._options.db_field_version = 1
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, None)
        Dummy._options.db_field_version = MigrationVersion(0, 1)
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.__class__.prop1._get_v0(dummy, type(dummy)), _attrs['prop1'])  # noqa
        self.assertEqual(dummy.__class__.prop1._get_v1(dummy, type(dummy)), missing)  # noqa
        self.assertEqual(dummy.prop1, _attrs['prop1'])
        self.assertEqual(dummy.__class__.prop1._get_v1(dummy, type(dummy)), missing)  # noqa
        del dummy.prop1
        self.assertEqual(dummy.__class__.prop1._get_v0(dummy, type(dummy)), missing)  # noqa
        self.assertEqual(dummy.__class__.prop1._get_v1(dummy, type(dummy)), missing)  # noqa
        self.assertEqual(dummy.prop1, None)
        dummy.prop1 = ['e', 'f']
        dummy.save()
        self.assertEqual(dummy.__class__.prop1._get_v0(dummy, type(dummy)), missing)  # noqa
        self.assertEqual(dummy.__class__.prop1._get_v1(dummy, type(dummy)), ['e', 'f'])  # noqa
        self.assertEqual(dummy.prop1, ['e', 'f'])
        Dummy._options.db_field_version = 1
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, ['e', 'f'])

    def test_specific_db_field_version(self):
        Dummy._options.db_field_version = 1
        Dummy.db_dt.version = 0
        _attrs = attrs.copy()
        _attrs['prop1'] = ['a', 'b', 'c']
        now = datetime.now()
        _attrs['db_dt'] = now
        dummy = Dummy.create(**_attrs)
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.prop1, _attrs['prop1'])
        self.assertEqual(dummy.db_dt, _attrs['db_dt'])
        key = dummy.get_finally_uuid()
        res = Dummy._options.db.db_get(key)
        self.assertEqual(res['db_dt'], _attrs['db_dt'])
        res = Dummy._options.db.db_get(key + '/prop1')
        self.assertEqual(res, _attrs['prop1'])

    def _test_multi_thread(self):
        dummy = Dummy.create(**attrs)

        def run():
            dummy.count += 1

        max_workers = 6
        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            fs = []
            for _ in xrange(max_workers):
                fs.append(exe.submit(run))

        as_completed(fs)

        self.assertEqual(dummy.count, 1)  # FIXME

    def _test_multi_thread0(self):

        max_workers = 6
        dummys = [Dummy.create(**attrs) for _ in xrange(max_workers)]

        def update(dummy):
            dummy.name = dummy.id
            dummy.save()

        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            fs = []
            for dummy in dummys:
                fs.append(exe.submit(update, dummy))

        as_completed(fs)

        dummys = Dummy.gets_by()

        for dummy in dummys:
            self.assertEqual(str(dummy.id), dummy.name)

    def test_noneable(self):
        dummy = Dummy.create(**attrs)
        dummy.password = None
        dummy.save()
        _attrs = attrs.copy()
        _attrs['foo'] = None
        Dummy.create(**_attrs)

    def test_atomic(self):
        dummy = Dummy.create(**attrs)
        old_age = dummy.age
        dummy.age = Dummy.age + 3
        dummy.save()
        self.assertEqual(dummy.age, old_age + 3)
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.age, old_age + 3)
        old_age = dummy.age
        dummy.update(age=Dummy.age + 3)
        self.assertEqual(dummy.age, old_age + 3)
        dummy = Dummy.get(dummy.id)
        self.assertEqual(dummy.age, old_age + 3)

    def test_validate(self):
        _attrs = attrs.copy()
        _attrs['name'] = 2333
        with self.assertRaises(ValidationError):
            _Dummy.create(**_attrs)
        _attrs['name'] = '999'
        _Dummy.create(**_attrs)
        _attrs['name'] = '9999'
        with self.assertRaises(ValidationError):
            _Dummy.create(**_attrs)
        old_validate = Dummy.olo_validate
        Dummy.olo_validate = Mock()
        Dummy.olo_validate.side_effect = ValidationError()
        with self.assertRaises(ValidationError):
            Dummy.create(name='test')
        self.assertEqual(Dummy.olo_validate.call_count, 1)
        Dummy.olo_validate = old_validate
        dummy = Dummy.create(name='test')
        self.assertIsNotNone(dummy)
        Dummy.olo_validate = Mock()
        Dummy.olo_validate.side_effect = ValidationError()
        with self.assertRaises(ValidationError):
            dummy.update(name='test1')
        self.assertEqual(Dummy.olo_validate.call_count, 1)
        Dummy.olo_validate = old_validate

    def test_clear_cache(self):
        dummy = Dummy.create(**attrs)
        with patched_execute as execute:
            dummy = Dummy.cache.get(dummy.id)
            self.assertTrue(execute.called)
        with patched_execute as execute:
            dummy = Dummy.cache.get(dummy.id)
            self.assertFalse(execute.called)
        dummy._clear_cache()
        with patched_execute as execute:
            dummy = Dummy.cache.get(dummy.id)
            self.assertTrue(execute.called)

    def test_alias_field(self):
        bar = Bar.create(name='1', age=2, xixi='hehe')
        bar = Bar.get_by(name=bar.name)
        self.assertEqual(bar.xixi, 'hehe')
        bar.xixi = 'wow'
        bar.save()
        bar = Bar.get_by(name=bar.name)
        self.assertEqual(bar.xixi, 'wow')
        t = Ttt.create()
        self.assertTrue(isinstance(t.time, datetime))
        t = Ttt.get(t.id)
        self.assertTrue(isinstance(t.time, datetime))
        jsn = t.to_json()
        self.assertTrue(isinstance(jsn['time'], str))

    def test_repr(self):
        bar = Bar.create(name='你好', age=2, xixi=u'世界')
        bar = Bar.get_by(name=bar.name)
        b = eval(repr(bar))
        self.assertEqual(bar.name, b.name)
        self.assertEqual(bar.age, b.age)
        self.assertEqual(bar.xixi, b.xixi)

    def test_instantiate(self):
        with self.assertRaises(RuntimeError):
            class Foo_(Foo):
                def __init__(self, name):
                    pass

        class _Foo(Foo):
            @override
            def __init__(self, name):
                super(_Foo, self).__init__(_olo_is_new=False)
                # context.in_model_instantiate
                self.name = name
                self._clone()

            @override
            def _clone(self):
                if self.__ctx__.in_model_instantiate:
                    return self
                return self.__class__(self.name)

        foo = _Foo('xixi')
        self.assertEqual(foo.name, 'xixi')

    def test_db_field_model(self):
        class Test(BaseModel):
            name = DbField(str)

            @override
            def __init__(self, id):
                super(Test, self).__init__(_olo_is_new=False)
                self.id = id

            @override
            def _clone(self):
                return self.__class__(self.id)

            def get_uuid(self):
                return '/tests/test_db_field_model/Test/%s' % self.id

        t = Test(1)
        t.update(name='test1')
        t = Test(2)
        t.update(name='test2')
        t = Test(1)
        self.assertEqual(t.name, 'test1')
        t = Test(2)
        self.assertEqual(t.name, 'test2')
        t.update(name='test3')
        t = Test(2)
        self.assertEqual(t.name, 'test3')

        class Test__(BaseModel):
            name = DbField(str)

            @override
            def __init__(self, id, name='test'):
                super(Test__, self).__init__()
                self.id = id
                if not self.name:
                    self.name = name
                    self.save()

            @override
            def _clone(self):
                return self.__class__(self.id)

            def get_uuid(self):
                return '/tests/test_db_field_model/Test__/%s' % self.id

        t = Test__(1)
        self.assertEqual(t.name, 'test')
        t.update(name='test1')
        t = Test__(1)
        self.assertEqual(t.name, 'test1')

    def _test_repr(self):
        import datetime  # noqa

        dummy = Dummy.create(**attrs)
        dummy.prop1 = ['a', 'b']
        dummy.save()
        # FIXME: decrypt a decrypted value will raise error
        d = eval(repr(dummy))
        self.assertEqual(d.id, dummy.id)
        self.assertEqual(d.prop1, dummy.prop1)

    def test_options(self):
        class _Dummy(Dummy):
            class Options:
                report = Mock()

        Dummy._options.report('xixi')
        _Dummy._options.report('xixi')
        self.assertTrue(_Dummy._options._report.called)

    def test_field_getter(self):
        f = Foo.create(test_getter=1)
        self.assertEqual(f.test_getter, 2)
        f.update(test_getter=2)
        f = Foo.get(f.id)
        self.assertEqual(f.test_getter, 3)

    def test_field_setter(self):
        f = Foo.create(test_setter=1)
        self.assertEqual(f.test_setter, 0)
        f.update(test_setter=3)
        self.assertEqual(f.test_setter, 2)
        f = Foo.get(f.id)
        self.assertEqual(f.test_setter, 2)
