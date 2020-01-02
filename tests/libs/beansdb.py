#!/usr/bin/env python
# encoding: utf-8
# pylint: disable=C0301
"""
__init__.py

Created by geng xinyue on 2011-02-14.
Copyright (c) 2011 douban.com. All rights reserved.
Last Update by hurricane <lilinghui@douban.com>.
"""

import time
import random
import warnings
import threading
import libmc


def log(s):
    print(s)


def iteritems(v):
    if hasattr(v, 'iteritems'):
        return v.iteritems()
    return v.items()


def falsefunc(*a, **kw):
    return False


def nonefunc(*a, **kw):
    return None


def emptyfunc(*a, **kw):
    return SetStub()


class SetStub(object):
    """
    Stub of the set class
    """
    empty = set()
    def __len__(self):
        return 0

    def __iter__(self):
        return self.empty.__iter__()

    def __contains__(self, key):
        return False

    def __getattr__(self, name):
        if name.startswith('is'):
            return falsefunc
        elif name == 'intersection':
            return emptyfunc
        elif not name.startswith('__'):
            return nonefunc
        return None


class ThreadedObject(object):
    def __init__(self, cls, *args, **kw):
        self.local = threading.local()
        self._args = (cls, args, kw)

        def creator():
            return cls(*args, **kw)
        self.creator = creator

    def __getstate__(self):
        return self._args

    def __setstate__(self, state):
        cls, args, kw = state
        self.__init__(cls, *args, **kw)

    def __getattr__(self, name):
        obj = getattr(self.local, 'obj', None)
        if obj is None:
            self.local.obj = obj = self.creator()
        return getattr(obj, name)


def fnv1a(s):
    from fnv1a import get_hash
    return get_hash(s) & 0xffffffff


MAX_KEYS_IN_GET_MULTI = 200
ONE_DAY = 24 * 3600
ONE_MINUTE = 60

RIVENDB_MC_PREFIX = 'rivendb:'
DB_MC_PREFIX = 'DB:'


class BeansDBError(IOError):
    msg_tmpl = 'BeansDB failed with key {key} on servers {servers}'

    def __init__(self, key, servers='unknown'):
        IOError.__init__(self)
        self.key = key
        self.servers = servers

    def __repr__(self):
        return self.msg_tmpl.format(key=self.key, servers=self.servers)

    __str__ = __repr__


class WriteFailedError(BeansDBError):
    msg_tmpl = 'write {key} failed({servers})'


class ReadFailedError(BeansDBError):
    msg_tmpl = 'read {key} failed({servers})'


class DeleteFailedError(BeansDBError):
    msg_tmpl = 'delete {key} failed({servers})'


def connect(server, **kwargs):
    comp_threshold = kwargs.pop('comp_threshold', 0)
    prefix = kwargs.pop('prefix', None)

    c = libmc.Client([server],
                     do_split=0,
                     comp_threshold=comp_threshold,
                     prefix=prefix)
    c.config(libmc.MC_CONNECT_TIMEOUT, 300)  # 0.3s
    c.config(libmc.MC_POLL_TIMEOUT, 3000)  # 3s
    c.config(libmc.MC_RETRY_TIMEOUT, 5)  # 5s
    return c


class MCStore(object):

    IGNORED_LIBMC_RET = frozenset([
        libmc.MC_RETURN_OK,
        libmc.MC_RETURN_INVALID_KEY_ERR
    ])

    def __init__(self, addr, threaded=True, **kwargs):
        self.addr = addr
        if threaded:
            self.mc = ThreadedObject(connect, addr, **kwargs)
        else:
            self.mc = connect(addr, **kwargs)

    def __repr__(self):
        return '<MCStore(addr=%s)>' % repr(self.addr)

    def __str__(self):
        return self.addr

    def set(self, key, data, rev=0):
        return bool(self.mc.set(key, data, rev))

    def set_raw(self, key, data, rev=0, flag=0):
        if rev < 0:
            raise Exception(str(rev))
        return self.mc.set_raw(key, data, rev, flag)

    def set_multi(self, values, return_failure=False):
        return self.mc.set_multi(values, return_failure=return_failure)

    def _check_last_error(self):
        last_err = self.mc.get_last_error()
        if last_err not in self.IGNORED_LIBMC_RET:
            raise IOError(last_err, self.mc.get_last_strerror())

    def get(self, key):
        try:
            r = self.mc.get(key)
            if r is None:
                self._check_last_error()
            return r
        except ValueError:
            self.mc.delete(key)

    def get_raw(self, key):
        r, flag = self.mc.get_raw(key)
        if r is None:
            self._check_last_error()
        return r, flag

    def get_multi(self, keys):
        r = self.mc.get_multi(keys)
        self._check_last_error()
        return r

    def delete(self, key):
        return bool(self.mc.delete(key))

    def delete_multi(self, keys, return_failure=False):
        return self.mc.delete_multi(keys, return_failure=return_failure)

    def exists(self, key):
        meta_info = self.mc.get('?' + key)
        if meta_info:
            version = meta_info.split(' ')[0]
            return int(version) > 0
        return False

    def incr(self, key, value):
        return self.mc.incr(key, int(value))


class BeansDBProxy(object):
    store_cls = MCStore
    threaded = True

    def __init__(self, proxies, rechoose_period=60, **kwargs):
        """Init.

        rechoose_period:
            Seconds to re-choose a proxy to communicate, to keep connection to
            two proxies.  Otherwise when one proxy fails, too many connect
            requests will overwhelm remaining proxies.

        """
        self.servers = [self.store_cls(i, threaded=self.threaded, **kwargs)
                        for i in proxies]
        # make the servers to be a random sequence
        random.shuffle(self.servers)
        self.rechoose_period = rechoose_period
        self._time_to_rechoose = time.time() + rechoose_period
        self.kwargs = kwargs

    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict['servers']
        server_addrs = [s.addr for s in self.servers]
        return odict, server_addrs

    def __setstate__(self, state):
        odict, server_addrs = state
        self.__dict__.update(odict)
        self.servers = [
            self.store_cls(i, threaded=self.threaded, **self.kwargs)
            for i in server_addrs]
        # make the servers to be a random sequence
        random.shuffle(self.servers)

    def _get_servers(self, key):
        now = time.time()
        if now > self._time_to_rechoose:
            # keep connection with the first two servers so that we do not
            # need to send SYN packet when the first server fails.
            self.servers = self.servers[:2][::-1] + self.servers[2:]
            self._time_to_rechoose = now + self.rechoose_period
        return self.servers[:2] # retry only twice

    def get(self, key, default=None):
        servers = self._get_servers(key)
        err_info = 'NO SERVER'
        for s in servers:
            try:
                r = s.get(key)
                if r is None:
                    r = default
                return r
            except IOError as e:
                self.servers = self.servers[1:] + self.servers[:1]
                err_info = e

        log('all backends read failed, with %s, err_info is %s' %
            (key, err_info))
        raise ReadFailedError(key, servers)

    def exists(self, key):
        for s in self._get_servers(key):
            try:
                return s.exists(key)
            except IOError:
                self.servers = self.servers[1:] + self.servers[:1]
        return False

    def mc_prefetch(self, keys):
        return {}

    def get_multi(self, keys, default=None):
        if len(keys) > MAX_KEYS_IN_GET_MULTI:
            r = self.get_multi(keys[:-MAX_KEYS_IN_GET_MULTI], default)
            r.update(self.get_multi(keys[-MAX_KEYS_IN_GET_MULTI:], default))
            return r
        err_info = 'NO SERVER'
        for s in self._get_servers(''):
            try:
                rs = s.get_multi(keys)
                if default is not None:
                    for k in keys:
                        if k not in rs:
                            rs[k] = default
                return rs
            except IOError as e:
                self.servers = self.servers[1:] + self.servers[:1]
                err_info = e

        log('all backends read failed, with %s, err_info is %s' %
            (str(keys), err_info))
        raise ReadFailedError(keys, self.servers)

    def set(self, key, value):
        if value is None:
            return False
        for i, s in enumerate(self._get_servers('')):
            if s.set(key, value):
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return True
        log('all backends set failed, with %s' % str(key))
        raise WriteFailedError(key)

    def set_multi(self, values):
        """
        set_multi will try every proxy until all keys have been set
        if all of proxies have been tried, but there are some keys are failed
        yet, record them in a exception and raise it.
        """
        for i, s in enumerate(self._get_servers('')):
            r, failures = s.set_multi(values, return_failure=True)
            if r:
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return True
            else:
                values = dict((k, values[k]) for k in failures)
        if failures:
            raise WriteFailedError(failures)

    def delete(self, key):
        for i, s in enumerate(self._get_servers('')):
            if s.delete(key):
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return True
        log('all backends delete failed, with %s' % str(key))
        raise DeleteFailedError(key)

    def delete_multi(self, keys):
        """
        delete_multi will try every proxy until all keys have been deleted.
        if all of proxies have been tried, but there are some keys are failed
        yet, record them in a exception and raise it.
        """
        for i, s in enumerate(self._get_servers('')):
            r, failures = s.delete_multi(keys, return_failure=True)
            if r:
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return True
            else:
                keys = failures
        if failures:
            raise DeleteFailedError(failures)

    def incr(self, key, value):
        if value is None:
            return
        for i, s in enumerate(self._get_servers(key)):
            v = s.incr(key, value)
            if v:
                if i > 0:
                    self.servers = self.servers[i:] + self.servers[:i]
                return v


class CacheWrapper(object):

    """a cached wrapper of BeansDBProxy"""
    def __init__(self, db, mc, delay_cleaner=None, expire_of_mc=None):
        self.db = db
        self.mc = mc
        if delay_cleaner is not None:
            warnings.warn('The delay_cleaner is deprecated and will be '
                          'removed soon. Please ping @panmiaocai if '
                          'seeing this.')

        self.delay_cleaner = delay_cleaner
        self.none_cache = SetStub()
        self.expire_of_mc = expire_of_mc if expire_of_mc else ONE_DAY

    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict['none_cache']
        return odict

    def __setstate__(self, odict):
        self.__dict__.update(odict)
        self.none_cache = SetStub()

    def clear_cache(self):
        self.none_cache = set()

    def __delete_multi_with_delay(self, keys):
        """
        delete_multi maybe useless in concurrence environment.
        so we need delay delete_multi to make sure it work.
        """
        # FIXME: reliable-delete-stream is required in case mc.delete fails
        # https://github.com/facebook/mcrouter/wiki/Features#reliable-delete-stream
        self.mc.delete_multi(keys)
        if self.delay_cleaner:
            for k in keys:
                self.delay_cleaner(k)

    def __set_multi_with_expire(self, values):
        """
        similar with __delete_multi_with_delay
        """
        to_delete = [k for k, v in iteritems(values) if v is None]
        to_set = {k: v for k, v in iteritems(values) if v is not None}
        if to_delete:
            self.mc.delete_multi(to_delete)
        if to_set:
            self.mc.set_multi(to_set, time=ONE_MINUTE)
        if self.delay_cleaner:
            for k in values:
                self.delay_cleaner(k)

    def __set_with_expire(self, key, value):
        """
        set(k, v, time) is the correct answer if no conflict occurs
        if another process/backend set with other expire time,
        set(k, v, time) will not work, so we need a delete(key, time) to
        cover this situation.
        """
        self.mc.set(key, value, time=ONE_MINUTE)
        if self.delay_cleaner:
            self.delay_cleaner(key)

    def __delete_with_delay(self, key):
        """
        similar with __set_with_expire
        """
        # FIXME: reliable-delete-stream is required in case mc.delete fails
        # https://github.com/facebook/mcrouter/wiki/Features#reliable-delete-stream
        self.mc.delete(key)
        if self.delay_cleaner:
            self.delay_cleaner(key)

    def get_setcache(self, key, empty_slot):
        """
        if mc has the key, return the value in mc.
        if db has the key, return the value in db and set into mc.
        else set a customed empty_slot into mc, and set expiration is a day duration.
        """
        if empty_slot is None:
            raise Exception('empty_slot should not be None')
        r = self.mc.get(key)
        if r is not None:
            return r
        else:
            # key is not in mc
            value = self.db.get(key)
            if value is not None:
                self.mc.set(key, value, time=self.expire_of_mc)
            else:
                value = empty_slot
                self.mc.add(key, value, time=self.expire_of_mc)
            return value

    def get(self, key, default=None):
        """
        if mc has the key, return the value in mc.
        else set a new value into mc, and set expiration is a day duration.
        """
        if key in self.none_cache:
            return default
        r = self.mc.get(key)
        if r is not None:
            return r
        else:
            # key is not in none_cache or mc
            value = self.db.get(key)
            if value is not None:
                self.mc.set(key, value, time=self.expire_of_mc)
            else:
                value = default
                self.none_cache.add(key)
            return value

    def exists(self, key):
        """
        exists is used to test whether the db has the key
        equal to db.get() is not None
        """
        if key in self.none_cache:
            return False
        r = self.mc.get(key)
        if r is not None:
            return True
        else:
            db_exist = self.db.exists(key)
            if not db_exist:
                self.none_cache.add(key)
            return db_exist

    def mc_prefetch(self, keys):
        return self.mc.get_multi(keys)

    def get_multi(self, keys, default=None):
        """
        just get the values, do not do anything to mc
        """
        keys_set = set(keys)
        non_cache_keys = keys_set.difference(self.none_cache)
        if not non_cache_keys:
            return {} if default is None else dict.fromkeys(keys, default)
        r = self.mc.get_multi(list(non_cache_keys))
        rs = dict((k, v) for k, v in iteritems(r) if v is not None)
        non_exist_keys = non_cache_keys.difference(rs)

        if non_exist_keys:
            # need origin results without use default value to set to mc.
            nrs = self.db.get_multi(list(non_exist_keys))
            if nrs:
                rs.update(nrs)
                self.mc.set_multi(nrs, time=self.expire_of_mc)
                # cache non_exist_keys in process
                non_exist_keys.difference_update(nrs)
            self.none_cache.update(non_exist_keys)
        if default is not None:
            # because the none_cache is just a stub fake, so we can not use it.
            non_get_keys = keys_set.difference(rs.keys())
            rs.update(dict.fromkeys(non_get_keys, default))
        return rs

    def set(self, key, value):
        """
        if value is None, it means delete.
        set will cause a set with expire, and a delayed delete.
        if db.set failed, should clean mc twice
        """
        try:
            if value is None:
                log('%s is deleted in both mc and db explicitly' % key)
                self.db.delete(key)
                self.none_cache.add(key)
                self.__delete_with_delay(key)
            else:
                # set value will not set none cache
                self.none_cache.discard(key)
                self.db.set(key, value)
                self.__set_with_expire(key, value)
            return True
        except:
            self.__delete_with_delay(key)
            raise

    def set_multi(self, values):
        try:
            # dicard all keys in none_cache
            self.none_cache.difference_update(values.keys())
            self.db.set_multi(values)
            self.__set_multi_with_expire(values)
            # because BeansDBProxy's set_multi will return True or raise
            return True
        except:
            self.__delete_multi_with_delay(values.keys())
            raise

    def delete(self, key):
        try:
            r = self.db.delete(key)
            self.none_cache.add(key)
            return r
        finally:
            self.__delete_with_delay(key)

    def delete_multi(self, keys):
        try:
            self.db.delete_multi(keys)
            self.none_cache.update(keys)
            return True
        finally:
            self.__delete_multi_with_delay(keys)

    def incr(self, key, value):
        if value is None:
            return None
        try:
            self.none_cache.discard(key)
            r = self.db.incr(key, value)
        finally:
            self.__delete_with_delay(key)
        return r

    def clear_thread_ident(self):
        self.mc.clear_thread_ident()
