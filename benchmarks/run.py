from olo_models import Ben as OBen
from peewee_models import Ben as PBen
from sqlalchemy_models import (
    Ben as SBen,
    session
)

from utils import timer as _timer
from config import get_mysql_conn

try:
    xrange
except NameError:
    xrange = range


conn = get_mysql_conn()


def setup():
    cur = conn.cursor()
    cur.execute('''
CREATE TABLE `ben` (
`id` int(10) NOT NULL AUTO_INCREMENT,
`name` varchar(255) NOT NULL,
`age` int(10) NOT NULL,
`key` varchar(255) NOT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8''')
    conn.commit()


def teardown():
    cur = conn.cursor()
    cur.execute('''drop table ben''')
    conn.commit()


def timer(name, init=False):
    if init:
        try:
            teardown()
            setup()
        except Exception:
            pass
    return _timer(name)


def create():
    n = 1000

    attrs = dict(age=1, name='a', key='b')

    with timer('%s times olo create' % n, True):
        for _ in xrange(n):
            OBen.create(**attrs)

    with timer('%s times peewee create' % n, True):
        for _ in xrange(n):
            PBen.create(**attrs)

    with timer('%s times sqlalchemy create' % n, True):
        for _ in xrange(n):
            ben = SBen(**attrs)
            s = session()
            s.add(ben)
            s.commit()


def query():
    n = 100

    with timer('%s times olo query' % n):
        for _ in xrange(n):
            OBen.query.filter(age=1).all()

    with timer('%s times peewee query' % n):
        for _ in xrange(n):
            list(PBen.select().where(PBen.age == 1))

    with timer('%s times sqlalchemy query' % n):
        s = session()
        for _ in xrange(n):
            s.query(SBen).filter(SBen.age == 1).all()


def update():
    n = 100

    age = 1
    with timer('%s times olo update' % n):
        ben = OBen.get_by(age=age)
        for age in xrange(age + 1, n + age + 1):
            ben.update(age=age)

    with timer('%s times peewee update' % n):
        ben = PBen.get(PBen.age == age)
        for age in xrange(age + 1, n + age + 1):
            ben.age = age
            ben.save()

    with timer('%s times sqlalchemy update' % n):
        s = session()
        ben = s.query(SBen).filter_by(age=age).first()
        for age in xrange(age + 1, n + age + 1):
            ben.age = age
            s.commit()


def _run():
    print('=' * 80)
    print('[benchmark create]')
    create()
    print('')
    print('=' * 80)
    print('[benchmark query]')
    query()
    print('')
    print('=' * 80)
    print('[benchmark update]')
    update()
    print('=' * 80)


def run():
    try:
        teardown()
    except Exception:
        pass
    try:
        setup()
        _run()
    finally:
        conn.close()


if __name__ == '__main__':
    run()
