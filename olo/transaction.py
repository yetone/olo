class Transaction(object):
    def __init__(self, db):
        self._db = db
        self._canceled = False

    def _begin(self):
        self._db.begin()

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

    def __enter__(self):
        self._orig_autocommit = self._db.autocommit
        self._db.autocommit = False
        self._begin()
        self._db.push_transaction(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        depth = self._db.transaction_depth

        try:
            if self._canceled or exc_type:
                self.rollback(False)
            elif depth == 1:
                try:
                    self.commit(False)
                except:
                    self.rollback(False)
                    raise
        finally:
            self._db.autocommit = self._orig_autocommit
            self._db.pop_transaction()
