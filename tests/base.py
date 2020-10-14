# coding=utf-8
import logging
from datetime import datetime, date
from enum import Enum
from uuid import uuid4

from olo import MySQLDataBase, PostgreSQLDataBase, Model, Field, DbField
from olo.debug import set_debug
from olo.logger import logger
from tests.fixture import (  # noqa pylint:disable=W
    TestCase, mc, beansdb,
    init_tables,
    MYSQL_HOST, MYSQL_PORT,
    MYSQL_USER, MYSQL_PASSWORD,
    MYSQL_DB, MYSQL_CHARSET,
    PGSQL_HOST, PGSQL_PORT,
    PGSQL_USER, PGSQL_PASSWORD,
    PGSQL_DB,
    is_pg)

#init_tables()
set_debug(True)
logger.setLevel(logging.DEBUG)
if is_pg:
    db = PostgreSQLDataBase(
        PGSQL_HOST, PGSQL_PORT,
        PGSQL_USER, PGSQL_PASSWORD,
        PGSQL_DB,
        beansdb=beansdb
    )
else:
    db = MySQLDataBase(
        MYSQL_HOST, MYSQL_PORT,
        MYSQL_USER, MYSQL_PASSWORD,
        MYSQL_DB,
        charset=MYSQL_CHARSET,
        beansdb=beansdb
    )
db.pool.enable_log = True


class _BaseModel(Model):
    __abstract__ = True

    class Options:
        db = db
        cache_client = mc
        enable_log = True
        table_engine = 'InnoDB'
        table_charset = 'latin1'


class BaseModel(_BaseModel):
    __abstract__ = True

    class Options:
        reason = 'test inherit'

    def __eq__(self, other):
        pk_name = self.get_singleness_pk_name()
        return (
            self.__class__ is other.__class__ and
            getattr(self, pk_name) == getattr(other, pk_name)
        )


class Gender(Enum):
    FEMALE = 0
    MALE = 1
    OTHER = 2


class Dummy(BaseModel):
    id = Field(int, primary_key=True, length=11, auto_increment=True)
    name = Field(str, noneable=True, default='dummy', length=255,
                 charset='utf8mb4')
    age = Field(int, default=12, on_update=lambda x: x.__class__.age + 1,
                length=11)
    password = Field(str, noneable=True, encrypt=True, length=511)
    flag = Field(int, default=0, noneable=True, choices=[0, 1, 2], length=5)
    tags = Field([str], default=[])
    payload = Field({str: [int]}, noneable=True, default={})
    foo = Field(int, noneable=True, length=11)
    dynasty = Field(str, default='现代', length=4, charset='utf8mb4')
    dynasty1 = Field(str, noneable=True, length=4)
    gender = Field(Gender, default=Gender.FEMALE)
    created_at = Field(
        datetime,
        default=datetime.now
    )
    updated_at = Field(
        datetime,
        default=datetime.now,
        on_update=datetime.now
    )
    created_date = Field(
        date,
        default=date.today
    )
    prop1 = DbField([str], noneable=True)
    count = DbField(int, default=0, choices=range(30),
                    on_update=lambda x: x.count + 3)
    db_dt = DbField(datetime, noneable=True)
    count1 = DbField(int, noneable=True)

    def get_uuid(self):
        return '/dummy/{}'.format(self.id)

    def after_update(self):
        after_update()

    def before_update(self, **attrs):
        before_update()

    @classmethod
    def before_create(cls, **attrs):
        before_create()

    @classmethod
    def after_create(cls, inst):
        after_create()


class Foo(BaseModel):
    id = Field(int, primary_key=True, auto_increment=True, length=11)
    name = Field(str, noneable=True, default='foo', length=255)
    age = Field(int, noneable=True, default=1, length=11)
    age_str = Field(int, noneable=True, default=1, output=str, length=11)
    key = Field(str, noneable=True, default=lambda: str(uuid4()), length=255)
    prop1 = DbField(list, noneable=True)
    boolean = Field(bool, default=False)
    test_getter = Field(int, default=0)
    test_setter = Field(int, default=0)

    @test_getter.getter
    def test_getter(self, value):
        return value + 1

    @test_setter.setter
    def test_setter(self, value):
        return value - 1

    __unique_keys__ = (
        (name, 'age'),
        ('key',)
    )

    __index_keys__ = (
        'age',
    )

    def get_uuid(self):
        return '/foo/{}'.format(self.id)


class Bar(BaseModel):
    name = Field(str, primary_key=True, length=255)
    age = Field(int, default=1, length=11)
    xixi = Field(str, name='key', default=lambda: str(uuid4()), length=255)
    word = Field(str, noneable=True, length=255)
    prop1 = DbField(list, noneable=True)

    class Options:
        table_charset = 'utf8mb4'

    __index_keys__ = (
        (),
        ('xixi', age),
        'age'
    )

    __order_bys__ = (
        'xixi',
        ('xixi', 'age'),
        ('-age', 'xixi'),
        ('word', 'age'),
        ('-age', 'word'),
    )

    def get_uuid(self):
        return '/foo/{}'.format(self.id)


class Ttt(BaseModel):
    id = Field(int, primary_key=True, output=str, auto_increment=True, length=11)
    time = Field(
        datetime,
        name='created_at',
        default=datetime.now
    )


class Lala(BaseModel):
    id = Field(int, primary_key=True, output=str, auto_increment=True, length=11)
    name = Field(str, length=255)
    age = Field(int, length=11)


db.create_all()


def after_update():
    pass


def before_update():
    pass


def after_create():
    pass


def before_create():
    pass
