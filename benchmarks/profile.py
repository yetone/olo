import cProfile

from benchmarks.olo_models import Ben as OBen
from benchmarks.peewee_models import Ben as PBen
from benchmarks.sqlalchemy_models import (
    Ben as SBen,
    session
)


if __name__ == '__main__':
    n = 100

    pr = cProfile.Profile()
    pr.enable()
    for _ in xrange(n):
        OBen.query.filter(age=1).all()
    pr.disable()
    pr.dump_stats('olo.prof')

    pr = cProfile.Profile()
    pr.enable()
    for _ in xrange(n):
        list(PBen.select().where(PBen.age == 1))
    pr.disable()
    pr.dump_stats('peewee.prof')

    pr = cProfile.Profile()
    pr.enable()
    s = session()
    for _ in xrange(n):
        s.query(SBen).filter(SBen.age == 1).all()
    pr.disable()
    pr.dump_stats('sqlalchemy.prof')
