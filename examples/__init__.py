import redis
import redis_batch
from redis.utils import HIREDIS_AVAILABLE
if HIREDIS_AVAILABLE:
    import hiredis
try:
    import asyncio_redis
    import asyncio_redis.pool
    ASYNCIO_REDIS_AVAILABLE = True
except ImportError:
    asyncio_redis = None
    ASYNCIO_REDIS_AVAILABLE = False
from redis_batch.parser import DefaultParser


def show_env_info():
    print("Redis version:", redis.__version__)
    print("Redis-Batch version:", redis_batch.__version__)
    print("asyncio_redis version:",
          getattr(asyncio_redis, '__version__', '???'))
    print("Hiredis available:", HIREDIS_AVAILABLE)
    if HIREDIS_AVAILABLE:
        import hiredis
        print("Hiredis version:", hiredis.__version__)
    print("redis_batch DefaultParser:", DefaultParser.__name__)
