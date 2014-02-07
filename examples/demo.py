"""
Simple example of BulkRedisClient and DualRedisClient usage
"""
import redis
import asyncio

from redis_batch.client import BatchRedisClient, DualRedisClient
from examples import show_env_info


def sync_call(client, cmd_name, *cmd_args):
    cmd = getattr(client, cmd_name)
    res1 = cmd(*cmd_args)
    res2 = cmd(*cmd_args)
    return [res1, res2]


@asyncio.coroutine
def async_call(client, cmd_name, *cmd_args):
    cmd = getattr(client, cmd_name)
    res1 = yield from cmd(*cmd_args)
    res2 = yield from cmd(*cmd_args)
    return [res1, res2]


@asyncio.coroutine
def call_mixin(client, cmd_name, *cmd_args):
    res1 = sync_call(client, cmd_name, *cmd_args)
    fut = async_call(client, 'async_' + cmd_name, *cmd_args)
    res2 = yield from fut
    return [res1, res2]


if __name__ == "__main__":
    show_env_info()

    sync_client = redis.Redis()
    event_loop = asyncio.get_event_loop()
    async_client = BatchRedisClient(loop=event_loop)
    dual_client = DualRedisClient(loop=event_loop)

    sync_call_resp = sync_call(sync_client, 'ping')
    async_call_resp = event_loop.run_until_complete(
        async_call(async_client, 'async_ping'))
    call_mixin_resp = event_loop.run_until_complete(
        call_mixin(dual_client, 'ping'))

    assert sync_call_resp == async_call_resp, 'Same response expected'
    assert call_mixin_resp[0] == sync_call_resp,  'Same response expected'
    assert call_mixin_resp[1] == async_call_resp, 'Same response expected'

