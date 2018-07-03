import threading
from collections import deque
from olo.utils import get_thread_ident


class Local(object):
    __slots__ = ('__storage__', '__ident_func__')

    def __init__(self):
        object.__setattr__(self, '__storage__', {})
        object.__setattr__(self, '__ident_func__', get_thread_ident)

    def __iter__(self):
        return iter(self.__storage__.items())

    def __release_local__(self):
        self.__storage__.pop(self.__ident_func__(), None)

    def __getattr__(self, name):
        try:
            return self.__storage__[self.__ident_func__()][name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        ident = self.__ident_func__()
        storage = self.__storage__
        try:
            storage[ident][name] = value
        except KeyError:
            storage[ident] = {name: value}

    def __delattr__(self, name):
        try:
            del self.__storage__[self.__ident_func__()][name]
        except KeyError:
            raise AttributeError(name)


class DbLocal(threading.local):
    def __init__(self, autocommit):
        super(DbLocal, self).__init__()
        self._autocommit = autocommit
        self._transactions = deque()
        self._beansdb_commands = deque()
        self._lazy_funcs = deque()
        self._commit_handlers = deque()
        self._rollback_handlers = deque()

    def start_beansdb_transaction(self):
        self._beansdb_commands.append(deque())

    def pop_beansdb_transaction(self):
        try:
            return self._beansdb_commands.pop()
        except IndexError:
            return deque()

    def shift_beansdb_transaction(self):
        try:
            return self._beansdb_commands.popleft()
        except IndexError:
            return deque()

    def append_beansdb_commands(self, *cmds):
        if not self._beansdb_commands:
            self.start_beansdb_transaction()
        self._beansdb_commands[-1].extend(cmds)

    def insert_beansdb_commands(self, *cmds):
        if not self._beansdb_commands:
            self.start_beansdb_transaction()
        self._beansdb_commands[0].extend(cmds)

    def add_lazy_func(self, func):
        self._lazy_funcs.append(func)

    def clear_lazy_funcs(self):
        self._lazy_funcs.clear()

    def add_commit_handler(self, handler):
        self._commit_handlers.append(handler)

    def clear_commit_handlers(self):
        self._commit_handlers.clear()

    def add_rollback_handler(self, handler):
        self._rollback_handlers.append(handler)

    def clear_rollback_handlers(self):
        self._rollback_handlers.clear()
