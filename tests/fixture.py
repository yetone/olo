# coding: utf-8

import os
import re
import glob
import socket
import logging
import unittest

import libmc
from tests.libs.beansdb import BeansDBProxy
from olo.compat import PY2


MAX_CONN = 103


in_travis = os.environ.get('ENV') == 'travis'


is_pg = os.getenv('DB') == 'pg'
SCHEMA_FILE = 'schema.sql'
if is_pg:
    SCHEMA_FILE = 'schema_pg.sql'
BEANSDB_CFG = {
    'localhost:11211': range(16),
}
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_DB = 'test_olo'
MYSQL_CHARSET = 'utf8mb4'

PGSQL_HOST = os.getenv('PGSQL_HOST', 'localhost')
PGSQL_PORT = 5432
PGSQL_USER = os.getenv('PGSQL_USER', 'root')
PGSQL_PASSWORD = os.getenv('PGSQL_PASSWORD', '')
PGSQL_DB = os.getenv('PGSQL_DB', 'test_olo')


created_tables = False
logger = logging.getLogger(__name__)

mc = libmc.Client(['localhost:11211'])
beansdb = BeansDBProxy(BEANSDB_CFG)
mysql_conn = None

approot = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..'
)
dbdir = os.path.join(approot, 'databases')
schema_path = os.path.join(dbdir, SCHEMA_FILE)


def init_tables():
    setup_mysql_conn()
    cur = mysql_conn.cursor()
    tables = get_all_tables(cur)

    if not tables:
        create_tables(cur)


def get_postgresql_conn():
    from psycopg2 import connect
    return connect(
        host=PGSQL_HOST,
        port=PGSQL_PORT,
        user=PGSQL_USER,
        password=PGSQL_PASSWORD,
        database=PGSQL_DB
    )


def get_mysql_conn():
    if is_pg:
        return get_postgresql_conn()

    try:
        from pymysql import connect
    except ImportError:
        from MySQLdb import connect

    return connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        db=MYSQL_DB,
        charset=MYSQL_CHARSET
    )


def setup_mysql_conn():
    global mysql_conn  # pylint: disable=W

    if mysql_conn is not None:
        mysql_conn.close()

    mysql_conn = get_mysql_conn()
    if not is_pg:
        cur = mysql_conn.cursor()
        cur.execute('SET GLOBAL sql_mode = ""')
        mysql_conn.commit()


def rollback_all():
    from tests.base import db
    with db.transaction():
        db.rollback()
    db.pool.clear_conns()
    mysql_conn.rollback()


def _mc_server_flush_all(host, port):
    sock = socket.socket()
    sock.connect((host, port))
    req = 'flush_all\r\n'
    expected_res = 'OK\r\n'
    if not PY2:
        req = bytes(req, 'utf8')
        expected_res = bytes(expected_res, 'utf8')
    assert len(req) == sock.send(req)
    assert sock.recv(1024) == expected_res
    sock.close()


def _flush_mc_server(_mc):
    # won't trigger on mc stub
    if not hasattr(_mc, 'stats'):
        return

    stats = _mc.stats()
    assert len(stats) == 1
    host, port = list(stats.keys())[0].split(':')
    port = int(port)
    _mc_server_flush_all(host, port)


def _clear_beansdb_for_test():
    for s in beansdb.servers:
        assert hasattr(s, 'mc')
        _flush_mc_server(s.mc)
        getattr(s.mc, 'clear', lambda: None)()


def get_all_tables(cur):
    if is_pg:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    else:
        cur.execute('SHOW TABLES')
    return [table for table, in cur]


def drop_tables(cur, tables):
    logger.info('drop all tables')
    for table in tables:
        if is_pg:
            cur.execute('DROP TABLE "%s"' % table)
        else:
            cur.execute('DROP TABLE `%s`' % table)
    cur.connection.commit()


def create_tables(cursor):
    if not os.path.isfile(schema_path):
        return
    logger.info('create all tables')
    for sql in get_sqls(open(schema_path)):
        cursor.execute(change_memory_engine(sql))
    cursor.connection.commit()


def truncate_tables(cur, tables):
    for table in tables:
        if is_pg:
            cur.execute('TRUNCATE TABLE "%s" RESTART IDENTITY' % table)
        else:
            cur.execute('TRUNCATE TABLE `%s`' % table)
    cur.connection.commit()


def get_sqls(fileobj):
    sql = ''
    for line in fileobj:
        sql += line
        if line.rstrip().endswith(';'):
            yield change_for_test(sql)
            sql = ''


class TestCase(unittest.TestCase):

    def setUp(self):
        _flush_mc_server(mc)
        mc.reset()
        _clear_beansdb_for_test()

        self.__setup_mysql()
        self.__setup_stubs()

    def __setup_stubs(self):
        try:
            import active_stubs
        except ImportError:
            pass
        else:
            active_stubs.setup()

    def __setup_mysql(self):
        setup_mysql_conn()

        rollback_all()

        cur = mysql_conn.cursor()
        if not is_pg:
            cur.execute('SET FOREIGN_KEY_CHECKS = 0')

        tables = get_all_tables(cur)

        truncate_tables(cur, tables)

        cur.connection.commit()

    def tearDown(self):
        self.__teardown_mysql()
        self.__teardown_stubs()

        from olo.libs.pool import ConnProxy
        self.assertLessEqual(
            ConnProxy.pid, MAX_CONN,
            'db connection count must less equal than {}'.format(MAX_CONN)
        )

    def __teardown_mysql(self):
        rollback_all()

    def __teardown_stubs(self):
        try:
            import active_stubs
        except ImportError:
            pass
        else:
            active_stubs.teardown()


def get_data_files():
    return [sqlfile for sqlfile in glob.glob(os.path.join(dbdir, '*.sql'))
            if not sqlfile.endswith(SCHEMA_FILE)]


def change_memory_engine(sql, blob_length=3000):
    return sql


def change_for_test(sql):
    return remove_fulltext_key(sql)


RE_FULLTEXT = re.compile(r'(?im)(,\n\s*FULLTEXT KEY[^,\n]*)(?=,|\n)')


def remove_fulltext_key(sql):
    # "FULLTEXT indexes are supported only for MyISAM tables ...'
    #  -- http://dev.mysql.com/doc/refman/5.0/en/create-index.html
    # so remove fulltext key
    return RE_FULLTEXT.sub('', sql)
