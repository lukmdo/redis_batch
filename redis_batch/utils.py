import time
import asyncio


class DelayedTask(asyncio.Task):
    """loop.call_later but for Tasks"""
    def __init__(self, delay, coro, *, loop=None):
        assert asyncio.iscoroutine(coro), repr(coro)
        asyncio.Future.__init__(self, loop=loop)
        self._coro = iter(coro)
        self._fut_waiter = None
        self._must_cancel = False
        self._loop.call_later(delay, self._step)
        self.__class__._all_tasks.add(self)


class DrainQueueBase(asyncio.Queue):
    """
    Base class for all sort DrainQueues with `drain` Task scheduling:

    Examples:
    - SizeDrainQueue - drain after queue gets full
    - TimeSizeDrainQueue - drain when oldest element > timeout or queue full
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.drain_tasks = set()
        self.active_drain_tasks = set()

    def _flush(self):
        return [self.get_nowait() for n in range(self.qsize())]

    @asyncio.coroutine
    def _drain(self, event_type, **kwargs):
        """scheduling boilerplate"""
        t = next(iter(self.drain_tasks))
        current = t.current_task(loop=self._loop)

        if self.cancel_drain(event_type, **kwargs):
            self.drain_tasks.remove(current)
            return

        # cancel unscheduled drain tasks
        self.active_drain_tasks.add(current)
        unscheduled = self.drain_tasks - self.active_drain_tasks
        [t.cancel() for t in unscheduled]
        self.drain_tasks.difference_update(unscheduled)

        # use q to enable puts and enable multiple drain tasks
        q = self._flush()
        yield from self.drain(q)
        self.drain_tasks.remove(current)
        self.active_drain_tasks.remove(current)

    @asyncio.coroutine
    def drain(self, q):
        raise NotImplementedError("consume the q queue here")

    def cancel_drain(self, event_type, **kwargs):
        """
        Called when the drain task starts. Implement cancel logic here for
        cases like `get_nowait` got called earlier. `False` by default.

        :rtype: bool
        """
        return False


class SizeDrainQueue(DrainQueueBase):
    """
    Schedules a `drain` queue Task when it gets full (`qsize` >= `maxsize`)
    """
    ET_SDRAIN = 1  # size drain

    def __init__(self, maxsize, *args, **kwargs):
        super().__init__(*args, maxsize=maxsize, **kwargs)

    def _put(self, item):
        self._queue.append(item)
        if self.full():
            self.drain_tasks.add(
                asyncio.Task(self._drain(self.ET_SDRAIN), loop=self._loop))

    def _flush(self):
        q = [self.get_nowait() for n in range(self.qsize())]
        if self.full():
            self.drain_tasks.add(
                asyncio.Task(self._drain(self.ET_SDRAIN), loop=self._loop))
        return q

    @asyncio.coroutine
    def drain(self, q):
        pass

    def cancel_drain(self, event_type, **kwargs):
        return not self.full()


class TimeSizeDrainQueue(DrainQueueBase):
    """
    Schedules a `drain` queue Task when:
    - the oldest element was not `get` or `get_nowait` before `timeout`
    - the queue gets full `qsize` >= `maxsize`.
    """
    ET_SDRAIN = 1  # size drain
    ET_TDRAIN = 2  # time drain

    def __init__(self, timeout, *args, **kwargs):
        """maxsize param is optional"""
        super().__init__(*args, **kwargs)
        self.timeout = timeout
        self.timestamp = None

    def _put(self, item):
        if self.empty():
            self.timestamp = time.time()
            self.drain_tasks.add(
                DelayedTask(self.timeout, self._drain(
                    self.ET_TDRAIN, timestamp=self.timestamp),
                    loop=self._loop))
        self._queue.append(item)
        if self.full():
            self.drain_tasks.add(
                asyncio.Task(self._drain(self.ET_SDRAIN), loop=self._loop))

    def _flush(self):
        q = [self.get_nowait() for n in range(self.qsize())]
        if self.full():
            self.drain_tasks.add(
                asyncio.Task(self._drain(self.ET_SDRAIN), loop=self._loop))
        elif not self.empty():
            self.timestamp = time.time()
            self.drain_tasks.add(
                DelayedTask(self.timeout, self._drain(
                    self.ET_TDRAIN, timestamp=self.timestamp),
                    loop=self._loop))
        return q

    @asyncio.coroutine
    def drain(self, q):
        pass

    def cancel_drain(self, event_type, timestamp=None, **kwargs):
        if event_type == self.ET_SDRAIN:
            return not self.full()
        elif event_type == self.ET_TDRAIN:
            return timestamp != self.timestamp

        raise ValueError('unknown event_type: {}'.format(event_type))


class PipeCommandQueue(TimeSizeDrainQueue):
    @asyncio.coroutine
    def drain(self, q):
        yield from self.pipe.execute_stack(q)
