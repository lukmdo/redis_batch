import sys
import asyncio

from redis.connection import BaseParser
from redis.connection import PythonParser
from redis._compat import (b, xrange, imap, byte_to_chr, unicode, bytes, long,
                           BytesIO, nativestr, basestring,
                           LifoQueue, Empty, Full)
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
from redis.connection import (
    SYM_STAR,
    SYM_DOLLAR,
    SYM_CRLF,
    SYM_LF,
    SYM_EMPTY,
)
from redis.utils import HIREDIS_AVAILABLE
if HIREDIS_AVAILABLE:
    import hiredis

__all__ = ['AsyncHiredisParser', 'AuthenticationError']


class AsyncPythonParser(PythonParser):
    def on_connect(self, connection):
        "Called when the socket connects"
        self._fp = connection.get_reader()
        if connection.decode_responses:
            self.encoding = connection.encoding

    def on_disconnect(self):
        "Called when the socket disconnects"
        self._fp = None

    @asyncio.coroutine
    def read(self, length=None):
        """
        Read a line from the socket if no length is specified,
        otherwise read ``length`` bytes. Always strip away the newlines.
        """
        # @TODO: rename to ._read (as its not part of BaseParser)
        # @TODO: make public interface of BaseParser explicit
        try:
            if length is not None:
                bytes_left = length + 2  # read the line ending
                if length > self.MAX_READ_LENGTH:
                    # apparently reading more than 1MB or so from a windows
                    # socket can cause MemoryErrors. See:
                    # https://github.com/andymccurdy/redis-py/issues/205
                    # read smaller chunks at a time to work around this
                    try:
                        buf = BytesIO()
                        while bytes_left > 0:
                            read_len = min(bytes_left, self.MAX_READ_LENGTH)
                            buf.write((yield from self._fp.read(read_len)))
                            bytes_left -= read_len
                        buf.seek(0)
                        return buf.read(length)
                    finally:
                        buf.close()
                return (yield from self._fp.read(bytes_left))[:-2]

            # no length, read a full line
            return (yield from self._fp.readline())[:-2]
        except Exception:
            e = sys.exc_info()[1]
            raise ConnectionError("Error while reading from socket: %s" %
                                  (e.args,))

    def process_response_data(self, response):
        # @TODO: suggest API change to add .process_response_data
        byte, response = byte_to_chr(response[0]), response[1:]

        if byte not in ('-', '+', ':', '$', '*'):
            raise InvalidResponse("Protocol Error: %s, %s" %
                                  (str(byte), str(response)))

        # server returned an error
        if byte == '-':
            response = nativestr(response)
            error = self.parse_error(response)
            # if the error is a ConnectionError, raise immediately so the user
            # is notified
            if isinstance(error, ConnectionError):
                raise error
            # otherwise, we're dealing with a ResponseError that might belong
            # inside a pipeline response. the connection's read_response()
            # and/or the pipeline's execute() will raise this error if
            # necessary, so just return the exception instance here.
            return error
        # single value
        elif byte == '+':
            pass
        # int value
        elif byte == ':':
            response = long(response)
        # bulk response
        elif byte == '$':
            length = int(response)
            if length == -1:
                return None
            response = self.read(length)
        # multi-bulk response
        elif byte == '*':
            length = int(response)
            if length == -1:
                return None
            response = [self.read_response() for i in xrange(length)]
        if isinstance(response, bytes) and self.encoding:
            response = response.decode(self.encoding)
        return response

    @asyncio.coroutine
    def read_response(self):
        response = yield from self.read()
        if not response:
            raise ConnectionError("Socket closed on remote end")
        return self.process_response_data(response)


class AsyncHiredisParser(BaseParser):
    "Parser class for connections using Hiredis"
    def __init__(self):
        if not HIREDIS_AVAILABLE:
            raise RedisError("Hiredis is not installed")

    def __del__(self):
        try:
            self.on_disconnect()
        except Exception:
            pass

    def on_connect(self, connection):
        "Called when the socket connects"
        self._fp = connection.get_reader()
        kwargs = {
            'protocolError': InvalidResponse,
            'replyError': ResponseError,
        }

        if connection.decode_responses:
            kwargs['encoding'] = connection.encoding
        self._reader = hiredis.Reader(**kwargs)

    def on_disconnect(self):
        self._fp = None
        self._reader = None

    @asyncio.coroutine
    def read_response(self):
        if not self._reader:
            raise ConnectionError("Socket closed on remote end")
        response = self._reader.gets()
        while response is False:
            try:
                buffer = yield from self._fp.read(4096)
            except Exception:
                e = sys.exc_info()[1]
                raise ConnectionError("Error while reading from socket: %s" %
                                      (e.args,))
            if not buffer:
                raise ConnectionError("Socket closed on remote end")
            self._reader.feed(buffer)
            # proactively, but not conclusively, check if more data is in the
            # buffer. if the data received doesn't end with \n, there's more.
            if not buffer.endswith(SYM_LF):
                continue
            response = self._reader.gets()
        if isinstance(response, ResponseError):
            response = self.parse_error(response.args[0])
        return response

if HIREDIS_AVAILABLE:
    DefaultParser = AsyncHiredisParser
else:
    DefaultParser = AsyncPythonParser
