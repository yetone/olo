from enum import Enum

from olo import Field
from tests.base import BaseModel, db
from tests.fixture import TestCase


class Gender(Enum):
    FEMALE = 0
    MALE = 1
    OTHER = 2


class EnumModel(BaseModel):
    id = Field(int, primary_key=True, auto_increment=True)
    gender = Field(Gender, default=Gender.FEMALE)


db.create_all()


class TestEnum(TestCase):
    def test_create(self):
        m = EnumModel.create()
        self.assertEqual(m.gender, Gender.FEMALE)
        m = EnumModel.create(gender=Gender.MALE)
        m = EnumModel.get(m.id)
        self.assertEqual(m.gender, Gender.MALE)

    def test_update(self):
        m = EnumModel.create(gender=Gender.MALE)
        m = EnumModel.get(m.id)
        m.gender = Gender.FEMALE
        m.save()
        m = EnumModel.get(m.id)
        self.assertEqual(m.gender, Gender.FEMALE)

    def test_query(self):
        EnumModel.create(gender=Gender.FEMALE)
        m = EnumModel.get_by(gender=Gender.FEMALE)
        self.assertEqual(m.gender, Gender.FEMALE)

    def test_in_query(self):
        m0 = EnumModel.create(gender=Gender.FEMALE)
        m1 = EnumModel.create(gender=Gender.MALE)
        m2 = EnumModel.create(gender=Gender.OTHER)
        ms = EnumModel.query.filter(EnumModel.gender.in_([Gender.FEMALE, Gender.MALE])).order_by(
            EnumModel.id.asc()).all()
        self.assertEqual([m0.id, m1.id], [m.id for m in ms])
