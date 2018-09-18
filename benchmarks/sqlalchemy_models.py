from sqlalchemy import (
    Column,
    Integer,
    String,
    create_engine
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from config import mysql_cfg


db_url = 'mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}'.format(
    **mysql_cfg
)


engine = create_engine(db_url)
Base = declarative_base()

session = sessionmaker()
session.configure(bind=engine)


class Ben(Base):
    __tablename__ = 'ben'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    key = Column(String)
