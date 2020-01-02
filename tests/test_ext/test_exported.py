from olo.ext.exported import exported_property

from tests.base import TestCase, Foo as _Foo


class TestExported(TestCase):

    def test_exported(self):
        class Foo(_Foo):
            @exported_property
            def new_age(self):
                return self.age + self.id

        foo = Foo.create()
        dct = foo.to_dict()
        self.assertEqual(dct['new_age'], foo.age + foo.id)
        self.assertEqual(dct['new_age'], foo.new_age)
