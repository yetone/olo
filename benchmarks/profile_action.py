import cProfile

from olo_models import Ben as OBen
from peewee_models import Ben as PBen
from sqlalchemy_models import (
    Ben as SBen,
    session
)

try:
    xrange
except NameError:
    xrange = range


if __name__ == '__main__':
    n = 100

    pr = cProfile.Profile()
    pr.enable()
    for _ in xrange(n):
        r = OBen.query.filter(age=1).all()
    print(len(r))
    pr.disable()
    pr.dump_stats('olo.prof')

    pr = cProfile.Profile()
    pr.enable()
    for _ in xrange(n):
        r = list(PBen.select().where(PBen.age == 1))
    print(len(r))
    pr.disable()
    pr.dump_stats('peewee.prof')

    pr = cProfile.Profile()
    pr.enable()
    s = session()
    for _ in xrange(n):
        r = s.query(SBen).filter(SBen.age == 1).all()
    print(len(r))
    pr.disable()
    pr.dump_stats('sqlalchemy.prof')
