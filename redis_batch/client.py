import redis
import asyncio

import redis_batch.pipeline
import redis_batch.parser
import redis_batch.connection
from redis_batch.utils import PipeCommandQueue

__all__ = ['BatchRedisClient', 'BatchStrictRedisClient',
           'DualRedisClient', 'DualStrictRedisClient']


class BatchStrictRedisClient(redis.StrictRedis):
    def __init__(self, loop, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 connection_pool=None, charset='utf-8',
                 errors='strict', decode_responses=False,
                 unix_socket_path=None,
                 cmd_maxsize=500,
                 cmd_timeout=0.01):
        self._loop = loop
        if not connection_pool:
            kwargs = {
                'loop': self._loop,
                'host': host,
                'port': port,
                'db': db,
                'password': password,
                'socket_timeout': socket_timeout,
                'encoding': charset,
                'encoding_errors': errors,
                'decode_responses': decode_responses,
                'parser_class': redis_batch.parser.DefaultParser
            }
            connection_pool = redis.ConnectionPool(
                connection_class=redis_batch.connection.AsyncConnection,
                **kwargs)

        self.connection_pool = connection_pool
        self.response_callbacks = self.RESPONSE_CALLBACKS

        # could be optionally external to client like the connection_pool
        command_queue = PipeCommandQueue(
            timeout=cmd_timeout, maxsize=cmd_maxsize, loop=self._loop)
        self._pipe = redis_batch.pipeline.AsyncStrictPipeline(
            command_queue,
            self.connection_pool,
            self.response_callbacks,
            transaction=True,
            shard_hint=None,
            loop=self._loop)

    def __getattr__(self, name):
        """compatibility: forward self.async_XXX calls to self.XXX calls"""
        if name.startswith("async_"):
            name = name.replace("async_", "", 1)
            return object.__getattribute__(self, name)
        else:
            return object.__getattribute__(self, name)

    def get_event_loop(self):
        return self._loop

    def execute_command(self, *args, **options):
        """put command on command stack"""
        fut = asyncio.Future(loop=self._loop)
        asyncio.Task(self._pipe.execute_command(
            fut, *args, **options), loop=self._loop)
        return fut


class BatchRedisClient(redis.Redis, BatchStrictRedisClient):
    pass


# could just take two clients and proxy...
def _dual_client_factory(client_class, async_client_class):

    class ClientClass(client_class):
        def __init__(self, loop, **kwargs):
            """Client supporting both [a]synchronous interfaces using same API:

            >>> resp = client.ping()                   # synchronous
            >>> resp = yield from client.async_ping()  # asynchronous

            The `async_XXX` calls return future object `asyncio.Task`
            """
            async_connection_pool = kwargs.pop('async_connection_pool', None)

            # async kwargs
            async_clinet_kwargs = kwargs.copy()
            async_clinet_kwargs['connection_pool'] = async_connection_pool
            self._async_client = async_client_class(
                loop, **async_clinet_kwargs)

            # redis-py
            super(ClientClass, self).__init__(**kwargs)

        def __getattr__(self, name):
            if name.startswith("async_"):
                name = name.replace("async_", "", 1)
                return object.__getattribute__(self._async_client, name)
            else:
                return object.__getattribute__(self, name)

    return ClientClass


DualStrictRedisClient = _dual_client_factory(
    redis.StrictRedis, BatchStrictRedisClient)
DualRedisClient = _dual_client_factory(redis.Redis, BatchRedisClient)
