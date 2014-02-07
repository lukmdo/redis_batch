"""
DIY profiling (manual exploratory tool)

kcps: kilo commands per second
"""
import time
import io
import pstats
import cProfile

import redis
import asyncio_redis
import asyncio_redis.pool

import asyncio
from redis_batch.client import BatchStrictRedisClient, DualStrictRedisClient
from examples import show_env_info


NUM_CALLS = 3000


def sync_call(client, n=10):
    out = [client.ping() for i in range(n)]
    return out


def sync_pipeline_call(client, n=10, pipe_maxsize=10):
    out = []
    pipe = client.pipeline()
    for i in range(0, n, pipe_maxsize):
        if i + pipe_maxsize < n:
            [pipe.ping() for j in range(pipe_maxsize)]
        else:
            [pipe.ping() for j in range(n - i)]
        out.append(pipe.execute())
    return out


@asyncio.coroutine
def async_gather_call(client, n=10):
    futures = [client.async_ping() for i in range(n)]
    out = yield from asyncio.gather(*futures, loop=client._loop)
    return out


@asyncio.coroutine
def asyncio_redis_call(client, loop, n=10):
    out = yield from asyncio.gather(
        *[client.ping() for i in range(n)], loop=loop)
    return out


def sync_async_mixin_call(dual_client, n=10):
    futures = [dual_client.async_ping() for i in range(n // 2)]
    out1 = [dual_client.ping() for i in range(n // 2)]
    out2 = yield from asyncio.gather(*futures)
    out = out1 + out2
    return out


def _format_profile(pr):
    s = io.StringIO()
    sortby = 'cumulative'
    ps = pstats.Stats(pr, stream=s).strip_dirs().sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())


def _format_call_info(text, delta, n=NUM_CALLS):
    print("{:<40} {:.6f} sec ({:.2f} kcps) ".format(
        text, delta, NUM_CALLS / delta / 1000))


def main(event_loop):
    show_env_info()

    sync_client = redis.Redis()
    iopool = yield from asyncio_redis.pool.Pool.create(poolsize=100)
    dual_client = DualStrictRedisClient(loop=event_loop)
    async_pipeline_client = BatchStrictRedisClient(
        loop=event_loop)

    @asyncio.coroutine
    def run_round(pr1=None, pr2=None, pr3=None):
        t0 = time.time()
        sync_call(sync_client, NUM_CALLS)
        delta = time.time() - t0
        _format_call_info("Sync Call", delta)

        t0 = time.time()
        sync_call(dual_client, NUM_CALLS)
        delta = time.time() - t0
        _format_call_info("Dual Sync Call", delta)

        t0 = time.time()
        sync_pipeline_call(sync_client, NUM_CALLS, 500)
        delta = time.time() - t0
        _format_call_info("Sync Pipelined Call", delta)

        t0 = time.time()
        yield from asyncio_redis_call(iopool, event_loop, NUM_CALLS)
        delta = time.time() - t0
        _format_call_info("ASYNC_REDIS Call", delta)

        t0 = time.time()
        # if pr3: pr3.enable()
        yield from async_gather_call(async_pipeline_client, NUM_CALLS)
        # if pr3: pr3.disable()
        delta = time.time() - t0
        _format_call_info("Async Gather Batch Call", delta)

        # t0 = time.time()
        # yield from sync_async_mixin_call(dual_client, NUM_CALLS)
        # delta = time.time() - t0
        # _format_call_info("Async/Sync Mixin Call", delta)

    pr1 = cProfile.Profile()
    pr2 = cProfile.Profile()
    pr3 = cProfile.Profile()
    pr1, pr2, pr3 = None, None, None

    print("\n---- 1st round ----\n")
    yield from run_round()

    print("\n---- 2nd round ----\n")
    yield from run_round()

    print("\n---- 3rd round ----\n")
    yield from run_round()

    print("\n---- 4th round ----\n")
    yield from run_round(pr1, pr2, pr3)  # samples profile metrics

    if pr1:
        print("\n------------------\n")
        _format_profile(pr1)

    if pr2:
        print("\n------------------\n")
        _format_profile(pr2)

    if pr3:
        print("\n------------------\n")
        _format_profile(pr3)


if __name__ == "__main__":
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(main(event_loop))
