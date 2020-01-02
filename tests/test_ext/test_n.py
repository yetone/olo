from olo.ext.n import n

from tests.base import TestCase, Foo as _Foo, Bar
from tests.utils import patched_execute


def prefetch_factory(model_class, default=None):
    def func(cls, query):
        return model_class.gets(
            [str(o.id) for o in query]
        )

    return n(default=default)(func)


class TestN(TestCase):

    def test_n(self):
        class Foo(_Foo):

            @n
            def a_bars(cls, foos):
                bars = Bar.gets([str(f.id) for f in foos])
                return bars

            @n
            def b_bars(cls, foos):
                bars = Bar.gets([str(f.id) for f in foos])
                return {
                    int(b.name): b
                    for b in bars
                }

            @n(1)
            def c_bars(cls, foos):
                bars = Bar.gets([str(f.id) for f in foos])
                return bars

            d_bars = prefetch_factory(Bar, default=1)

            @n
            @classmethod
            def f_bars(cls, foos):
                bars = Bar.gets([str(f.id) for f in foos])
                return bars

        Foo.create(age=1)
        Foo.create(age=2)
        Foo.create(age=3)
        Foo.create(age=4)
        Foo.create(age=5)
        Foo.create(age=6)
        Bar.create(name=1)
        Bar.create(name=2)
        Bar.create(name=3)

        def a_func():
            fs = Foo.gets_by()
            for f in fs:
                b = f.a_bar

                if f.id > 3:
                    self.assertIsNone(b)
                else:
                    self.assertIsInstance(b, Bar)
                    self.assertEqual(str(f.id), b.name)

        def b_func():
            fs = Foo.gets_by()
            for f in fs:
                b = f.b_bar
                if f.id > 3:
                    self.assertIsNone(b)
                else:
                    self.assertIsInstance(b, Bar)
                    self.assertEqual(str(f.id), b.name)

        def c_func():
            fs = Foo.gets_by()
            for f in fs:
                b = f.c_bar
                if f.id > 3:
                    self.assertEqual(b, 1)
                else:
                    self.assertIsInstance(b, Bar)
                    self.assertEqual(str(f.id), b.name)

        def d_func():
            fs = Foo.gets_by()
            for f in fs:
                b = f.d_bar
                if f.id > 3:
                    self.assertEqual(b, 1)
                else:
                    self.assertIsInstance(b, Bar)
                    self.assertEqual(str(f.id), b.name)

        def e_func():
            fs = Foo.gets_by()
            fs = Foo.cache.gets([f.id for f in fs])
            for f in fs:
                b = f.d_bar
                if f.id > 3:
                    self.assertEqual(b, 1)
                else:
                    self.assertIsInstance(b, Bar)
                    self.assertEqual(str(f.id), b.name)

        def f_func():
            fs = Foo.gets_by()
            f = Foo.cache.get(fs[0].id)
            b = f.d_bar
            if f.id > 3:
                self.assertEqual(b, 1)
            else:
                self.assertIsInstance(b, Bar)
                self.assertEqual(str(f.id), b.name)

        def g_func():
            fs = Foo.gets_by()
            f = Foo.cache.get_by(key=fs[0].key)
            b = f.d_bar
            if f.id > 3:
                self.assertEqual(b, 1)
            else:
                self.assertIsInstance(b, Bar)
                self.assertEqual(str(f.id), b.name)

        def h_func():
            fs = Foo.gets_by()
            for f in fs:
                b = f.f_bar
                if f.id > 3:
                    self.assertIsNone(b)
                else:
                    self.assertIsInstance(b, Bar)
                    self.assertEqual(str(f.id), b.name)

        with patched_execute as exe:
            a_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            b_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            c_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            d_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            e_func()
            self.assertEqual(exe.call_count, 3)

        with patched_execute as exe:
            e_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            f_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            f_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            g_func()
            self.assertEqual(exe.call_count, 3)

        with patched_execute as exe:
            g_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            h_func()
            self.assertEqual(exe.call_count, 2)

        with patched_execute as exe:
            h_func()
            self.assertEqual(exe.call_count, 2)
