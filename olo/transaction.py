from __future__ import annotations

import threading

from functools import wraps
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from olo.database import BaseDataBase


class Transaction(threading.local):
    def __init__(self, db: BaseDataBase):
        super(Transaction, self).__init__()
        self._db = db
        self._entered = False
        self._canceled = False
        self._curs = set()
        self.conn = None

    def _begin(self):
        self._entered = True
        self._db.begin()

    def get_curs(self):
        return self._curs

    def add_cur(self, cur):
        self._curs.add(cur)

    def cancel(self):
        self._canceled = True

    def commit(self, begin=True):
        self._db.commit()
        if begin:
            self._begin()

    def rollback(self, begin=True):
        self._db.rollback()
        if begin:
            self._begin()

    def __call__(self, func):
        @wraps(func)
        def _(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return _

    def __enter__(self):
        if self._db.in_transaction():
            return self._db.get_last_transaction()

        self._orig_autocommit = self._db.autocommit
        self._db.autocommit = False
        self._begin()
        self._db.push_transaction(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._entered:
            return

        self._entered = False
        depth = self._db.transaction_depth

        try:
            if self._canceled or exc_type:
                self.rollback(False)
            elif depth == 1:
                try:
                    self.commit(False)
                except:  # noqa
                    self.rollback(False)
                    raise
        finally:
            self._db.autocommit = self._orig_autocommit
            self._db.pop_transaction()

            while len(self._curs) != 0:
                self._curs.pop()

            if self.conn is not None:
                conn = self.conn
                self.conn = None
                conn.release()
