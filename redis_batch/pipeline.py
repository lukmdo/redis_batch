import sys
import itertools

import redis
import redis.client
import asyncio
from redis.client import b, izip
from redis.exceptions import (
    ConnectionError,
    ResponseError,
    WatchError,
    ExecAbortError,
)


SYM_EMPTY = b('')


class AsyncBasePipeline(redis.client.BasePipeline):
    def __init__(self, stack, *args, loop=None, **kwargs):
        self.command_stack = stack
        stack.pipe = self
        self._loop = loop
        # Full implementation would maintain a set of queues
        # and put to the wright one.
        # This uses only one cmd-queue. consider making cmd-queue
        # be optionally external to client like the connection_pool.
        super().__init__(*args, **kwargs)

    def reset(self):
        command_stack = self.command_stack  # self.command_stack.clear?
        super().reset()
        self.command_stack = command_stack

    @asyncio.coroutine
    def pipeline_execute_command(self, fut, *args, **options):
        yield from self.command_stack.put((fut, args, options))
        return self

    @asyncio.coroutine
    def execute_stack(self, stack, raise_on_error=True):
        """Execute all the commands from the given `stack`"""
        if not stack:
            return []
        if self.scripts:
            self.load_scripts()
        if self.transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        conn = self.connection_pool.get_connection('MULTI', self.shard_hint)

        try:
            return (yield from execute(conn, stack, raise_on_error))
        except ConnectionError:
            conn.disconnect()
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a WatchError, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issue
            # will fail, propegating the real ConnectionError
            if self.watching:
                raise WatchError("A ConnectionError occured on while watching "
                                 "one or more keys")
            # otherwise, it's safe to retry since the transaction isn't
            # predicated on any state
            return (yield from execute(conn, stack, raise_on_error))
        finally:
            self.reset()
            self.connection_pool.release(conn)

    @asyncio.coroutine
    def _execute_transaction(self, connection, commands, raise_on_error):
        cmds = itertools.chain(
            [(None, ('MULTI', ), {})],
            commands,
            [(None, ('EXEC', ), {})])
        all_cmds = SYM_EMPTY.join(
            itertools.starmap(
                connection.pack_command,
                [args for f, args, options in cmds]))
        yield from connection.send_packed_command(all_cmds)
        errors = []

        # parse off the response for MULTI
        # NOTE: we need to handle ResponseErrors here and continue
        # so that we read all the additional command messages from
        # the socket
        try:
            # yield from self.parse_response(connection, '_')
            yield from connection.read_response()
        except ResponseError:
            errors.append((0, sys.exc_info()[1]))

        # and all the other commands
        for i, command in enumerate(commands):
            try:
                # yield from self.parse_response(connection, '_')
                yield from connection.read_response()
            except ResponseError:
                ex = sys.exc_info()[1]
                self.annotate_exception(ex, i + 1, command[0])
                errors.append((i, ex))

        # parse the EXEC.
        try:
            # response = yield from self.parse_response(connection, '_')
            response = yield from connection.read_response()
        except ExecAbortError:
            if self.explicit_transaction:
                self.immediate_execute_command('DISCARD')
            if errors:
                raise errors[0][1]
            raise sys.exc_info()[1]

        if response is None:
            raise WatchError("Watched variable changed.")

        # put any parse errors into the response
        for i, e in errors:
            response.insert(i, e)

        if len(response) != len(commands):
            self.connection.disconnect()
            raise ResponseError("Wrong number of response items from "
                                "pipeline execution")

        # find any errors in the response and raise if necessary
        if raise_on_error:
            self.raise_first_error(commands, response)

        # We have to run response callbacks manually
        for r, cmd in izip(response, commands):
            fut, args, options = cmd
            if isinstance(r, Exception):
                fut.set_exception(r)
            else:
                command_name = args[0]
                if command_name in self.response_callbacks:
                    r = self.response_callbacks[command_name](r, **options)
                fut.set_result(r)


class AsyncStrictPipeline(AsyncBasePipeline, redis.StrictRedis):
    pass
