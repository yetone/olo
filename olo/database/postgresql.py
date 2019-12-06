import logging
import re
from typing import Tuple, Optional

from olo.logger import logger
from olo.database import BaseDataBase, OLOCursor
from olo.database.mysql import MySQLConnProxy
from olo.libs.pool import Pool
from olo.sql_ast_translators.postgresql_sql_ast_translator import PostgresSQLSQLASTTranslator


PATTERN_INDEX_DEF = re.compile('CREATE (?P<unique>UNIQUE )?INDEX (?P<name>.*) ON .* (?P<btree>USING btree )?\\((?P<fields>.*?)\\)')  # noqa


def create_conn(host: str, port: int, user: str, password: str, dbname: str, charset: str):
    from psycopg2 import connect
    return connect(database=dbname, user=user, password=password, host=host, port=port)


class PostgreSQLCursor(OLOCursor):
    def get_last_rowid(self):
        import psycopg2
        try:
            rows = self.fetchone()
            return rows[0]
        except psycopg2.ProgrammingError:
            return

    def log(self, sql: str, params: Optional[Tuple] = None, level: int = logging.INFO) -> None:
        logger.log(msg=self.mogrify(sql, params), level=level)


class PostgreSQLConnProxy(MySQLConnProxy):
    def ping(self):
        import psycopg2
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT 1')
            return True
        except psycopg2.OperationalError:
            pass
        return False


class PostgreSQLDataBase(BaseDataBase):
    ast_translator = PostgresSQLSQLASTTranslator()

    def __init__(self, host, port, user, password, dbname,
                 charset='utf8mb4',
                 beansdb=None, autocommit=True,
                 report=lambda *args, **kwargs: None,
                 max_active_size=10,
                 max_idle_size=5):

        super().__init__(
            beansdb=beansdb,
            autocommit=autocommit,
            report=report
        )
        self.pool = Pool(
            lambda: create_conn(
                host, port, user, password, dbname, charset
            ),
            conn_proxy_cls=PostgreSQLConnProxy,
            max_active_size=max_active_size,
            max_idle_size=max_idle_size,
        )

    def get_conn(self):
        return self.pool.acquire_conn()

    def get_cursor(self):  # pylint: disable=W
        assert self.in_transaction(), 'db.get_cursor must in transaction!'

        tran = self.get_last_transaction()
        conn = tran.conn
        if conn is None:
            conn = tran.conn = self.get_conn()

        cur = conn.cursor()
        return PostgreSQLCursor(cur, self)

    def get_tables(self):
        if self._tables is None:
            try:
                with self.transaction():
                    c = self.get_cursor()
                    c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                self._tables = {t for t, in c}
            except Exception:  # pragma: no cover
                return set()  # pragma: no cover
        return self._tables

    def get_index_rows(self, table_name):
        pk_name = f'{table_name}_pkey'
        if table_name not in self._index_rows_mapping:
            try:
                tables = self.get_tables()
                if table_name not in tables:
                    return []
                with self.transaction():
                    c = self.get_cursor()
                    c.execute('SELECT indexdef from pg_indexes where schemaname = %s and tablename = %s', (
                        'public', table_name
                    ))
                    res = []
                    for indexdef, in c:
                        m = PATTERN_INDEX_DEF.search(indexdef)
                        if not m:
                            continue
                        key_name = m.group('name')
                        unique = m.group('unique') is not None
                        btree = m.group('btree') is not None
                        fields = m.group('fields').split(',')
                        for idx, f in enumerate(fields):
                            f = f.strip()
                            res.append((
                                table_name,
                                1 if not unique else 0,
                                'PRIMARY' if key_name == pk_name else key_name,
                                idx + 1,
                                f,
                                'A',
                                0, None, None, '',
                                'BTREE' if btree else '',
                                '', ''))
                self._index_rows_mapping[table_name] = res
            except Exception:  # pragma: no cover
                return []  # pragma: no cover
        return self._index_rows_mapping[table_name]
