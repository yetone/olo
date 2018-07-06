# pylint: disable=W0703

import json
import logging
from functools import wraps

from olo.local import DbLocal
from olo.transaction import Transaction
from olo.logger import logger
from olo.errors import DataBaseError
from olo.compat import str_types, unicode
from olo.sql_ast_translators.mysql_sql_ast_translator import MySQLSQLASTTranslator


def need_beansdb(func):
    @wraps(func)
    def _(self, *args, **kwargs):
        if self._beansdb is None:
            raise DataBaseError(
                'Need beansdb client in database configure!'
            )
        return func(self, *args, **kwargs)
    return _


def need_beansdb_commit(func):
    @wraps(func)
    def _(self, *args, **kwargs):
        res = func(self, *args, **kwargs)
        if self.autocommit:
            self.commit_beansdb()
        return res
    return _


def sql_literal_factory(db):
    def literal(v):
        if isinstance(v, unicode):
            v = v.encode('utf-8')
        return db.literal(v)
    return literal


def log_sql(cur, sql, params=None, level=logging.INFO):
    db = cur._get_db()
    literal = sql_literal_factory(db)
    msg_tpl = '[SQL]: {}'
    if params is None:
        msg = msg_tpl.format(sql)
    else:
        if not isinstance(params, (list, tuple, dict)):
            params = (params,)
        msg = msg_tpl.format(sql % tuple(map(literal, params)))
    logger.log(msg=msg, level=level)


class OLOCursor(object):
    def __init__(self, cur, db):
        self.cur = cur
        self.db = db

    def __getattr__(self, name):
        return getattr(self.cur, name)

    @property
    def is_modified(self):
        return self.cur.is_modified  # pragma: no cover

    @is_modified.setter
    def is_modified(self, item):
        self.cur.is_modified = item

    def __iter__(self):
        return iter(self.cur)

    def __str__(self):
        return '<OLOCursor: {}>'.format(self.cur)  # pragma: no cover

    __repr__ = __str__

    def execute(self, sql, *args, **kwargs):
        if isinstance(self.db, DataBase):
            kwargs['called_from_store'] = kwargs.pop('called_from_store', True)  # pragma: no cover
        r = self.cur.execute(sql, *args, **kwargs)
        params = args[0] if args else None
        if self.db.enable_log:
            log_sql(self.cur, sql, params)
        return r

    def close(self):
        return self.cur.close()  # pragma: no cover

    def _get_db(self):
        return self.cur._get_db()


def get_sqls(lines):
    sql = ''
    for line in lines:
        sql += line
        if line.rstrip().endswith(';'):
            yield sql
            sql = ''


class BaseDataBase(object):

    ast_translator = MySQLSQLASTTranslator()

    def __init__(self, beansdb=None, autocommit=True,
                 report=lambda *args, **kwargs: None):
        self._beansdb = beansdb
        self._local = DbLocal(autocommit=autocommit)
        self.report = report
        self._tables = None
        self._index_rows_mapping = {}
        self.enable_log = False
        self._models = []

    def add_lazy_func(self, func):
        self._local.add_lazy_func(func)

    def _run_lazy_funcs(self):
        while True:
            try:
                func = self._local._lazy_funcs.popleft()
            except IndexError:
                return

            try:
                func()
            except Exception:
                self.report()

    def add_commit_handler(self, handler):
        self._local.add_commit_handler(handler)

    def _run_commit_handlers(self):
        while True:
            try:
                handler = self._local._commit_handlers.popleft()
            except IndexError:
                return

            try:
                handler()
            except Exception:
                self.report()

    def add_rollback_handler(self, handler):
        self._local.add_rollback_handler(handler)

    def _run_rollback_handlers(self):
        while True:
            try:
                handler = self._local._rollback_handlers.popleft()
            except IndexError:
                return

            try:
                handler()
            except Exception:  # pragma: no cover
                self.report()  # pragma: no cover

    def get_tables(self):
        if self._tables is None:
            c = self.get_cursor()
            try:
                c.execute('SHOW TABLES')
                self._tables = {t for t, in c}
            except Exception:  # pragma: no cover
                return set()  # pragma: no cover
        return self._tables

    def get_index_rows(self, table_name):
        if table_name not in self._index_rows_mapping:
            try:
                tables = self.get_tables()
                if table_name not in tables:
                    return []
                c = self.get_cursor()
                c.execute('SHOW INDEX FROM `{}`'.format(
                    table_name
                ))
                self._index_rows_mapping[table_name] = c.fetchall()
            except Exception:  # pragma: no cover
                return []  # pragma: no cover
        return self._index_rows_mapping[table_name]

    def get_cursor(self):
        raise NotImplementedError

    def gen_tables_schema(self):
        raise NotImplementedError

    def create_all(self):
        schema = self.gen_tables_schema()
        with self.transaction():
            for sql in get_sqls(schema.split('\n')):
                self.sql_execute(sql)

    def register_model(self, model_cls):
        self._models.append(model_cls)

    def log(self, sql, params=None, level=logging.INFO):
        if not self.enable_log:
            return
        cur = self.get_cursor()
        log_sql(cur, sql, params=params, level=level)

    def beansdb_log(self, cmd, args, kwargs, level=logging.INFO):
        if not self.enable_log:
            return

        def mapper(x):
            if isinstance(x, str_types):
                return x

            try:
                return json.dumps(x)
            except Exception:
                return '<UNKOWN>'

        try:
            msg = '[BEANSDB]: {} {}'.format(cmd, ' '.join(map(mapper, args)))
            logger.log(msg=msg, level=level)
        except Exception:  # pragma: no cover
            pass  # pragma: no cover

    @property
    def autocommit(self):
        return self._local._autocommit

    @autocommit.setter
    def autocommit(self, autommit):
        self._local._autocommit = autommit

    def begin(self):
        pass

    def sql_execute(self, sql, params=None):
        raise NotImplementedError

    def sql_commit(self):
        raise NotImplementedError

    def sql_rollback(self):
        raise NotImplementedError

    def execute(self, sql, params=None):
        self.log(sql, params)
        return self.sql_execute(sql, params)

    def commit_beansdb(self):
        self._do_beansdb_commands()

    def commit(self):
        res = self.sql_commit()
        self.log('COMMIT')
        self.commit_beansdb()
        self._run_lazy_funcs()
        self._run_commit_handlers()
        return res

    def rollback(self):
        res = self.sql_rollback()
        self.log('ROLLBACK')
        self._local.pop_beansdb_transaction()
        self._local.clear_lazy_funcs()
        self._run_rollback_handlers()
        return res

    def push_transaction(self, transaction):
        self._local._transactions.append(transaction)

    def pop_transaction(self):
        self._local._transactions.pop()

    def cancel_transaction(self):
        if not self.in_transaction():
            return False
        for tran in self._local._transactions:
            tran.cancel()
        return True

    @property
    def transaction_depth(self):
        return len(self._local._transactions)

    def in_transaction(self):
        return self.transaction_depth > 0

    def transaction(self):
        return Transaction(self)

    @need_beansdb
    def db_get(self, k, default=None):
        return self._beansdb.get(k, default)

    @need_beansdb
    def db_get_multi(self, ks):
        return self._beansdb.get_multi(ks)

    @need_beansdb_commit
    @need_beansdb
    def db_set(self, *args, **kwargs):
        self._local.append_beansdb_commands(('set', args, kwargs))

    @need_beansdb_commit
    @need_beansdb
    def db_set_multi(self, *args, **kwargs):
        self._local.append_beansdb_commands(('set_multi', args, kwargs))

    @need_beansdb_commit
    @need_beansdb
    def db_delete(self, *args, **kwargs):
        self._local.append_beansdb_commands(('delete', args, kwargs))

    @need_beansdb_commit
    @need_beansdb
    def db_delete_multi(self, *args, **kwargs):
        self._local.append_beansdb_commands(('delete_multi', args, kwargs))

    def _do_beansdb_commands(self):
        if not self._beansdb:
            return
        while True:
            cmds = self._local.shift_beansdb_transaction()
            if not cmds:
                break
            try:
                for cmd, args, kwargs in cmds:
                    func = getattr(self._beansdb, cmd)
                    func(*args, **kwargs)
                    self.beansdb_log(cmd, args, kwargs)
            except Exception:
                self._local.insert_beansdb_commands(*cmds)
                break


class DataBase(BaseDataBase):
    def __init__(self, store, beansdb=None, autocommit=True,
                 report=lambda *args, **kwargs: None):
        super(DataBase, self).__init__(
            beansdb=beansdb,
            autocommit=autocommit,
            report=report
        )
        self._store = store

    @property
    def store(self):
        return self._store

    def get_cursor(self):
        cur = self.store.get_cursor()  # pragma: no cover
        if cur is None:  # pragma: no cover
            return cur  # pragma: no cover
        return OLOCursor(cur, self)  # pragma: no cover

    def gen_tables_schema(self):
        raise NotImplementedError("not implement gen_tables_schema!")

    def sql_execute(self, sql, params=None):
        return self.store.execute(sql, params)

    def sql_commit(self):
        return self.store.commit()

    def sql_rollback(self):
        return self.store.rollback()
