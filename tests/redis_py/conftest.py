import pytest
import redis
import asyncio
from redis_batch.client import DualRedisClient, DualStrictRedisClient


def _get_client(cls, request=None, *args, **kwargs):
    params = {'host': 'localhost', 'port': 6379, 'db': 9}
    params.update(kwargs)

    client = cls(*args, **params)
    client.flushdb()

    if request:
        request.addfinalizer(client.flushdb)
    return client

def skip_if_server_version_lt(min_version):
    version = _get_client(redis.Redis).info()['redis_version']
    c = "StrictVersion('%s') < StrictVersion('%s')" % (version, min_version)
    return pytest.mark.skipif(c)

@pytest.fixture()
def r(request, **kwargs):
    loop = asyncio.get_event_loop()
    client = _get_client(DualRedisClient, request, loop, **kwargs)
    request.addfinalizer(loop.close)

    return client

@pytest.fixture()
def sr(request, **kwargs):
    loop = asyncio.get_event_loop()
    client = _get_client(DualStrictRedisClient, request, loop, **kwargs)
    request.addfinalizer(loop.close)

    return client