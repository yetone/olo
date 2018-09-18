from olo import Field, DbField
from olo.ext.declared import declared_attr

from tests.base import TestCase, BaseModel, Foo as _Foo


class TestDeclared(TestCase):

    def test_declared_attr(self):
        class Foo(_Foo):
            @declared_attr
            def new_age(cls):
                return DbField(int)

        self.assertTrue('new_age' in Foo.__all_fields__)
        foo = Foo.create(new_age=1, age_str='1')
        foo = Foo.get(foo.id)
        self.assertEqual(foo.new_age, 1)

        class Foo(BaseModel):
            id = Field(int, primary_key=True)
            name = Field(str, default='')
            age_str = Field(str, noneable=True)

            @declared_attr
            def age(cls):
                return Field(int)

            @declared_attr
            def key(cls):
                return Field(str, default='key')

        foo = Foo.create(age=1, age_str='1')
        foo = Foo.get(foo.id)
        self.assertEqual(foo.age, 1)

        class F_O_O(BaseModel):
            id = Field(int, primary_key=True)
            name = Field(str, default='')
            age_str = Field(str, noneable=True)
            key = Field(str, noneable=True)

            @declared_attr
            def age(cls):
                return Field(int)

            @declared_attr
            def __table_name__(cls):
                return cls.__name__.lower().replace('_', '')

        foo = F_O_O.create(age=2, key='xixi', age_str='2')
        foo = F_O_O.get(foo.id)
        self.assertEqual(foo.age, 2)
        self.assertEqual(foo.key, 'xixi')
        dct = foo.to_dict()
        self.assertTrue('age' in dct)
