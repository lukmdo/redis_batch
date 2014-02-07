import sys
import asyncio

from redis.connection import Connection
from redis.exceptions import (
    RedisError,
    ConnectionError,
    BusyLoadingError,
    ResponseError,
    InvalidResponse,
    AuthenticationError,
    NoScriptError,
    ExecAbortError,
)

__all__ = ['AsyncConnection']


class AsyncConnection(Connection):
    def __init__(
            self,
            loop=None,
            **kwargs):
        super().__init__(**kwargs)
        self._loop = loop
        self._reader = None
        self._writer = None

    def get_event_loop(self):
        return self._loop

    def get_reader(self):
        return self._reader

    def get_writer(self):
        return self._writer

    def disconnect(self):
        "Disconnects from the Redis server"
        self._parser.on_disconnect()
        if self._writer:
            self._writer.close()
        self._reader = None
        self._writer = None

    @asyncio.coroutine
    def connect(self):
        "Connects to the Redis server if not already connected"
        if self._writer:
            return
        try:
            self._reader, self._writer = yield from asyncio.open_connection(
                self.host, self.port, loop=self._loop)
        except Exception:
            e = sys.exc_info()[1]
            raise ConnectionError(self._error_message(e))

        try:
            self.on_connect()
        except RedisError:
            # clean up after any error in on_connect
            self.disconnect()
            raise

    @asyncio.coroutine
    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"
        if not self._writer:
            yield from self.connect()
        try:
            self._writer.write(command)
            yield from self._writer.drain()
        except Exception:
            e = sys.exc_info()[1]
            self.disconnect()
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." %
                                  (_errno, errmsg))
        except:
            self.disconnect()
            raise

    @asyncio.coroutine
    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = yield from self._parser.read_response()
        except:
            self.disconnect()
            raise
        if isinstance(response, ResponseError):
            raise response
        return response

    # def _error_message(self, exception): TODO
