# coding: utf-8

# system packages
import hashlib
import json
# third-party packages
from redis import ConnectionPool
from redis import Redis

# own packages


class RedisConnector(object):
    def __init__(self, **kwargs):
        self.connector = self.create_connection(**kwargs)

    def create_connection(self, **kwargs):
        pool = ConnectionPool(host=kwargs["host"], port=kwargs["port"],
                              db=kwargs["db"],
                              max_connections=kwargs["max_connection"])
        return Redis(connection_pool=pool)

    def get_session(self):
        return self.connector


_g = []


def connect(**config):
    name = hashlib.sha224(json.dumps(config)).hexdigest()
    key = "_redis_{0}".format(name)
    if key not in _g:
        client = RedisConnector(**config)
        _g[key] = client
    return _g[key]