from peewee import (
    MySQLDatabase,
    Model,
    CharField,
    IntegerField,
)

from config import mysql_cfg


mysql_cfg = dict(mysql_cfg)
dbname = mysql_cfg.pop('db')
db = MySQLDatabase(dbname, **mysql_cfg)


class BaseModel(Model):
    class Meta:
        database = db


class Ben(BaseModel):
    name = CharField()
    age = IntegerField()
    key = CharField()
