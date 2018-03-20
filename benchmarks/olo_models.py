
from olo import MySQLDataBase, Model, Field
from config import (  # noqa pylint:disable=W
    MYSQL_HOST, MYSQL_PORT,
    MYSQL_USER, MYSQL_PASSWORD,
    MYSQL_DB, MYSQL_CHARSET,
)


db = MySQLDataBase(
    MYSQL_HOST, MYSQL_PORT,
    MYSQL_USER, MYSQL_PASSWORD,
    MYSQL_DB,
    charset=MYSQL_CHARSET,
)


class BaseModel(Model):
    class Options:
        db = db


class Ben(BaseModel):
    id = Field(int, primary_key=True)
    name = Field(str)
    age = Field(int)
    key = Field(str)
