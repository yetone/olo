import time


class TimerFunc(object):
    def __init__(self, func, timer_):
        self.func = func
        self.timer = timer_

    def __call__(self, *args, **kwargs):
        with self.timer:
            return self.func(*args, **kwargs)


class timer(object):
    def __init__(self, name=None, ring=False):
        self.name = name
        self.ring = ring

    def __call__(self, func):
        return TimerFunc(func, self)

    def __enter__(self):
        self.st = time.time()
        if self.ring:
            print('[%s] running...' % self.name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('[%s] cost [%sms]' % (self.name, (time.time() - self.st) * 1000))
