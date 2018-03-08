DEBUG = False


def set_debug(v):
    global DEBUG

    DEBUG = v


def debug(s):
    if not DEBUG:
        return  # pragma: no cover

    print(s)
