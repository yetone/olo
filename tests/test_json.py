from olo import Field, select
from olo.field import JSONField
from tests.base import BaseModel, db
from tests.fixture import TestCase


class JSONModel(BaseModel):
    id = Field(int, primary_key=True, auto_increment=True)
    payload = JSONField()
    arr = JSONField(default=['a'])


db.create_all()


class TestJSON(TestCase):
    def test_create(self):
        payload = {'a': {'b': ['x', 'y'], 'c': 1, 'd': 'xxx'}, 'e': 1.1, 'f': True}
        m = JSONModel.create(payload=payload)
        self.assertEqual(m.payload, payload)
        m0 = JSONModel.get(m.id)
        self.assertEqual(m0.payload, payload)

    def test_eq_query(self):
        payload = {'a': {'b': ['x', 'y'], 'c': 1, 'd': 'xxx'}, 'e': 1.1, 'f': True}
        m0 = JSONModel.create(payload=payload)
        m1 = JSONModel.query.filter(JSONModel.payload['a']['c'] == 1).first()
        self.assertEqual(m1.id, m0.id)
        m2 = JSONModel.query.filter(JSONModel.payload['a']['d'] == 'xxx').first()
        self.assertEqual(m2.id, m0.id)
        m3 = JSONModel.query.filter(JSONModel.payload['e'] == 1.1).first()
        self.assertEqual(m3.id, m0.id)
        m4 = JSONModel.query.filter(JSONModel.payload['f'] == True).first()
        self.assertEqual(m4.id, m0.id)
        m5 = JSONModel.query.filter(JSONModel.payload['f'] == False).first()
        self.assertIsNone(m5)
        m6 = JSONModel.query.filter(JSONModel.payload['e'] == 1.2).first()
        self.assertIsNone(m6)

    def test_ne_query(self):
        payload = {'a': {'b': ['x', 'y'], 'c': 1, 'd': 'xxx'}, 'e': 1.1, 'f': True}
        m0 = JSONModel.create(payload=payload)
        payload = {'a': {'b': ['x', 'y'], 'c': 2, 'd': 'yyy'}, 'e': 1.2, 'f': False}
        m1 = JSONModel.create(payload=payload)
        m2 = JSONModel.query.filter(JSONModel.payload['a']['c'] != 2).first()
        self.assertEqual(m2.id, m0.id)
        m3 = JSONModel.query.filter(JSONModel.payload['a']['d'] != 'xxx').first()
        self.assertEqual(m3.id, m1.id)

    def test_contains_query(self):
        payload = {'a': {'b': ['x', 'y'], 'c': 1, 'd': 'xxx'}, 'e': 1.1, 'f': True}
        m0 = JSONModel.create(payload=payload)
        payload = {'a': {'b': ['z', 'a'], 'c': 2, 'd': 'yyy'}, 'e': 1.2, 'f': False}
        m1 = JSONModel.create(payload=payload)
        m2 = JSONModel.query.filter(JSONModel.payload['a']['b'].contains_('x')).first()
        self.assertEqual(m2.id, m0.id)
        m3 = JSONModel.query.filter(JSONModel.payload['a']['b'].not_contains_('x')).first()
        self.assertEqual(m3.id, m1.id)
        m4 = select(x for x in JSONModel if 'x' in x.payload['a']['b']).first()
        self.assertEqual(m4.id, m0.id)
        m5 = select(x for x in JSONModel if 'x' not in x.payload['a']['b']).first()
        self.assertEqual(m5.id, m1.id)

    def test_intersection_query(self):
        payload = {'a': {'b': ['x', 'y'], 'c': 1, 'd': 'xxx'}, 'e': 1.1, 'f': True}
        m0 = JSONModel.create(payload=payload)
        payload = {'a': {'b': ['z', 'a'], 'c': 2, 'd': 'yyy'}, 'e': 1.2, 'f': False}
        m1 = JSONModel.create(payload=payload)
        m2 = JSONModel.query.filter(JSONModel.payload['a']['b'] & ['x', 'c']).first()
        self.assertIsNone(m2)
        m3 = JSONModel.query.filter(JSONModel.payload['a']['b'] & ['x', 'z']).first()
        self.assertIsNone(m3)
        m4 = JSONModel.query.filter(JSONModel.payload['a']['b'] & ['z', 'b']).first()
        self.assertIsNone(m4)
        m5 = JSONModel.query.filter(JSONModel.payload['a']['b'] & ['x', 'y']).first()
        self.assertEqual(m5.id, m0.id)
        m6 = JSONModel.query.filter(JSONModel.payload['a']['b'] & ['a', 'z']).first()
        self.assertEqual(m6.id, m1.id)

    def test_union_query(self):
        payload = {'a': {'b': ['x', 'y'], 'c': 1, 'd': 'xxx'}, 'e': 1.1, 'f': True}
        m0 = JSONModel.create(payload=payload)
        payload = {'a': {'b': ['z', 'a'], 'c': 2, 'd': 'yyy'}, 'e': 1.2, 'f': False}
        m1 = JSONModel.create(payload=payload)
        m3 = JSONModel.query.filter(JSONModel.payload['a']['b'] | ['x', 'y']).first()
        self.assertEqual(m3.id, m0.id)
        m4 = JSONModel.query.filter(JSONModel.payload['a']['b'] | ['z', 'b']).first()
        self.assertEqual(m4.id, m1.id)

    def test_update(self):
        payload = {'a': {'b': ['x', 'y'], 'c': 1, 'd': 'xxx'}, 'e': 1.1, 'f': True}
        m0 = JSONModel.create(payload=payload)
        m1 = JSONModel.get(m0.id)
        m1.payload['a']['b'].append('z')
        m1.save()
        m2 = JSONModel.get(m0.id)
        self.assertEqual(['x', 'y', 'z'], m2.payload['a']['b'])

    def test_arr(self):
        payload = {'a': {'b': ['x', 'y'], 'c': 1, 'd': 'xxx'}, 'e': 1.1, 'f': True}
        m0 = JSONModel.create(payload=payload, arr=['a', 'b'])
        m1 = JSONModel.query.filter(JSONModel.arr.contains_('x')).first()
        self.assertIsNone(m1)
        m2 = JSONModel.query.filter(JSONModel.arr.contains_('a')).first()
        self.assertEqual(m2.id, m0.id)
