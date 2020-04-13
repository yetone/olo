import logging
import re
from enum import Enum
from typing import Tuple, Optional, List

from olo.logger import logger
from olo.database import BaseDataBase, OLOCursor
from olo.database.mysql import MySQLConnProxy
from olo.libs.pool import Pool
from olo.sql_ast_translators.postgresql_sql_ast_translator import PostgresSQLSQLASTTranslator
from olo.sql_ast_translators.sql_ast_translator import AST
from olo.utils import camel2underscore

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
        logger.log(msg=self.mogrify(sql, params).decode('utf8'), level=level)


class PostgreSQLConnProxy(MySQLConnProxy):
    def ping(self):
        import psycopg2
        try:
            cur = self.conn.cursor()
            cur.execute('SELECT 1')
            return True
        except psycopg2.OperationalError as e:
            logger.error('ping pg connection failed: %s', str(e))
        return False


class PostgreSQLDataBase(BaseDataBase):
    ast_translator = PostgresSQLSQLASTTranslator()

    def __init__(self, host, port, user, password, dbname,
                 charset='utf8mb4',
                 beansdb=None, autocommit=True,
                 report=lambda *args, **kwargs: None,
                 pool_size=5,
                 pool_timeout=30,
                 pool_recycle=60 * 60,
                 pool_max_overflow=10):

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
            size=pool_size,
            timeout=pool_timeout,
            recycle=pool_recycle,
            max_overflow=pool_max_overflow,
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
                    c.execute('SELECT table_name FROM information_schema.tables WHERE table_schema = %s', ('public',))
                self._tables = {t for t, in c}
            except Exception:  # pragma: no cover
                return set()  # pragma: no cover
        return self._tables

    def get_fields(self, table_name: str) -> List[Tuple[str, type, Optional[int]]]:
        fields = []
        try:
            with self.transaction():
                c = self.get_cursor()
                c.execute('SELECT column_name, data_type, character_maximum_length FROM information_schema.COLUMNS '
                          'WHERE table_name = %s', (table_name,))
                for c_name, data_type, data_length in c:
                    f_type = str
                    if data_type in ('integer', 'smallint'):
                        f_type = int
                    fields.append((c_name, f_type, data_length))
        except Exception as e:  # pragma: no cover
            logger.error('get fields from %s failed: %s', table_name, str(e))
        return fields

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

    def to_db_types_sql_asts(self) -> List[AST]:
        asts = []

        existing_enums = {}
        with self.transaction():
            rv = self.execute(
                'select t.typname, e.enumlabel from pg_type as t join pg_enum as e on t.oid = e.enumtypid'
            )
            for name, label in rv:
                existing_enums.setdefault(name, []).append(label)

        seen = set()

        for model in self._models:
            for k in model.__sorted_fields__:
                f = getattr(model, k)

                if isinstance(f.type, type) and issubclass(f.type, Enum):
                    enum_name = camel2underscore(f.type.__name__)

                    if enum_name in seen:
                        continue

                    seen.add(enum_name)

                    enum_labels = existing_enums.get(enum_name)

                    if enum_labels is None:
                        enum_labels = list(f.type.__members__)
                        asts.append([
                            'CREATE_ENUM',
                            enum_name,
                            enum_labels,
                        ])
                        continue

                    for member_name in f.type.__members__:
                        if member_name not in enum_labels:
                            asts.append([
                                'ADD_ENUM_LABEL',
                                enum_name,
                                enum_labels[-1] if enum_labels else '',
                                member_name
                            ])
                            enum_labels.append(member_name)
        return asts
