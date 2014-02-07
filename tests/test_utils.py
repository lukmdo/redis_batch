import asyncio
import unittest
import unittest.mock
from functools import partial

from redis_batch.utils import SizeDrainQueue, TimeSizeDrainQueue


if __name__ == "__main__":
    unittest.main()


class QueueTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._l = asyncio.get_event_loop()

    @classmethod
    def tearDownClass(cls):
        asyncio.set_event_loop(cls._l)

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.wait_for = partial(asyncio.wait_for, loop=self.loop)
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()


class TestSizeDrainQueue(QueueTestBase):

    @unittest.mock.patch.object(SizeDrainQueue, 'drain')
    def test_drain_full(self, drain_m):
        q = SizeDrainQueue(maxsize=2, loop=self.loop)
        wait_timeout = 0.005

        @asyncio.coroutine
        def test():
            yield from q.put(10)
            yield from q.put(20)
            self.assertEqual(q.qsize(), 2)

            t = asyncio.Task(q.put(30), loop=self.loop)
            self.assertEqual(drain_m.call_count, 0)
            yield from self.wait_for(t, wait_timeout)
            self.assertEqual(drain_m.call_count, 1)
            self.assertEqual(t.done(), True)
            self.assertEqual(q.qsize(), 1)
            self.assertEqual(q.get_nowait(), 30)

        self.loop.run_until_complete(test())

    def test_put_after_full(self):
        q = SizeDrainQueue(maxsize=2, loop=self.loop)

        @asyncio.coroutine
        def test():
            yield from q.put(10)
            yield from q.put(20)
            self.assertEqual(q.qsize(), 2)
            self.assertEqual(q.full(), True)
            yield from q.put(30)
            self.assertEqual(q.qsize(), 1)
            self.assertEqual(q.get_nowait(), 30)

        self.loop.run_until_complete(test())

    def test_multi_drain(self):
        class SizeSlowSumDrainQueue(SizeDrainQueue):
            SLEEP_TIME = 0.01

            def drain(self, q):
                yield from asyncio.sleep(self.SLEEP_TIME, loop=self._loop)
                yield from self.put(sum(q))

        @asyncio.coroutine
        def test():
            q = SizeSlowSumDrainQueue(maxsize=2, loop=self.loop)

            v1, v2, v3, v4 = 5, 20, 100, 1
            yield from q.put(v1)
            yield from q.put(v2)

            self.assertEqual(q.qsize(), 2)
            self.assertEqual(q.full(), True)
            yield from q.put(v3)
            self.assertEqual(q.qsize(), 1)
            self.assertEqual(q.get_nowait(), v3)
            q.put_nowait(v3)  # put back
            yield from q.put(v4)
            self.assertEqual(q.qsize(), 2)  # v3, v4

            yield from asyncio.sleep(q.SLEEP_TIME, loop=self.loop)
            self.assertEqual(q.qsize(), 1)
            self.assertEqual(q.get_nowait(), v1 + v2)
            q.put_nowait(v1 + v2)  # put back
            yield from asyncio.sleep(q.SLEEP_TIME, loop=self.loop)
            self.assertEqual(q.qsize(), 0)
            yield from asyncio.sleep(q.SLEEP_TIME, loop=self.loop)
            self.assertEqual(q.qsize(), 1)
            self.assertEqual(q.get_nowait(), v1 + v2 + v3 + v4)

        self.loop.run_until_complete(test())


class TestTimeSizeDrainQueue(QueueTestBase):

    @unittest.mock.patch.object(TimeSizeDrainQueue, 'drain')
    def test_drain_timeout(self, drain_m):
        timeout = 0.01
        wait_timeout = 0.002
        q = TimeSizeDrainQueue(timeout=timeout, maxsize=20, loop=self.loop)

        @asyncio.coroutine
        def test():
            yield from q.put(10)
            yield from q.put(20)
            self.assertEqual(q.qsize(), 2)

            t = asyncio.Task(q.put(30), loop=self.loop)
            yield from self.wait_for(t, wait_timeout)
            self.assertEqual(t.done(), True)
            self.assertEqual(q.qsize(), 3)
            self.assertEqual(drain_m.call_count, 0)
            yield from asyncio.sleep(timeout, loop=self.loop)
            self.assertEqual(drain_m.call_count, 1)
            self.assertEqual(q.qsize(), 0)

        self.loop.run_until_complete(test())


# @TODO: add TestDelayedTask
