from mock import patch
from contextlib import contextmanager
from .base import db


class AE(Exception):
    pass


@contextmanager
def auto_use_cache_ctx(*clses):
    try:
        for cls in clses:
            cls._options.auto_use_cache = True
        yield
    finally:
        for cls in clses:
            cls._options.auto_use_cache = False


@contextmanager
def no_cache_client(cls):
    cli = cls._options.cache_client
    try:
        cls._options.cache_client = None
        yield
    finally:
        cls._options.cache_client = cli


@contextmanager
def no_pk(cls):
    pk = cls.__primary_key__
    try:
        cls.__primary_key__ = ()
        yield
    finally:
        cls.__primary_key__ = pk


orig_execute = db.execute
orig_db_get = db.db_get
orig_db_get_multi = db.db_get_multi

patched_execute = patch('tests.base.db.execute', side_effect=orig_execute)
patched_db_get = patch('tests.base.db.db_get', side_effect=orig_db_get)
patched_db_get_multi = patch('tests.base.db.db_get_multi', side_effect=orig_db_get_multi)
