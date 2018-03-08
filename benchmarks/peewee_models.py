from peewee import (
    MySQLDatabase,
    Model,
    CharField,
    IntegerField,
)

from benchmarks.config import mysql_cfg


dbname = mysql_cfg.pop('db')
db = MySQLDatabase(dbname, **mysql_cfg)


class BaseModel(Model):
    class Meta:
        database = db


class Ben(BaseModel):
    name = CharField()
    age = IntegerField()
    key = CharField()
