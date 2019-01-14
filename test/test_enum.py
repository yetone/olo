from .base import TestCase, Haha, ColorEnum


class TestModel(TestCase):
    def test_create(self):
        obj = Haha.create(
            color=ColorEnum.blue,
        )
        self.assertEqual(obj.color, ColorEnum.blue)

        obj2 = Haha.get(obj.id)
        self.assertEqual(obj2.color, ColorEnum.blue)

        key = obj.get_finally_uuid()
        res = Haha._options.db.db_get(key + '/color')
        self.assertEqual(res, ColorEnum.blue.value)
