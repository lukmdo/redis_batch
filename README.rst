redis-py-UNAMEIT
================

::

    Throughput != Latency

Extension for `redis-py`_ that queues commands in backend to execute
them in batches.

Client side `MULTI`_ and `EXEC`_ wrapping or `pipelining`_ targeting xN
increase in throughput.

.. code-block:: bash

    time ((echo -en "MULTI\r\nPING\r\nPING\r\nEXEC\r\n"; sleep 1) |nc localhost 6379)
    time ((echo -en "PING\r\nPING\r\n"; sleep 1) |nc localhost 6379)

API changes/extending:

.. code-block:: python

    # make it feel like one pipeline
    redis_client = DualRedisClient(cmd_timeout=0.002, cmd_maxsize=100)

    future1 = redis_client.async_info() # redis.FutureResult
    result  = redis_client.ping()       # direct result
    future2 = redis_client.async_ping() # redis.FutureResult
    ...
    redis_client.wait(f1, f2) # wait + batch other redis_client.async_ calls
    print(result, f1.result(), f2.result())

    redis_client.async_ping(timeout=None) # to call later 

As shown above its more than syntax to `existing pipelining`_

Note (proof of concept) and be welcome to join:

::

    Development Status :: 1 - Planning

Backend:

-  `asyncio`_/`tulip`_
-  `threadeding`_
-  `celery`_

More examples (after `installing/building Redis server`_):

.. code-block:: pycon

    >>> py.test tests/redis_py/  # runs all redis-py tests on DualClient
    >>> python examples/demo.py  # show how to use BulkRedis and DualClient
    >>> python examples/prof.py  # naive profiler

.. _redis-py: https://pypi.python.org/pypi/redis/
.. _MULTI: http://redis.io/commands/multi
.. _EXEC: http://redis.io/commands/exec
.. _pipelining: http://redis.io/topics/pipelining
.. _existing pipelining: https://pypi.python.org/pypi/redis/#pipelines
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html
.. _tulip: https://pypi.python.org/pypi/asyncio/0.3.1
.. _threadeding: http://docs.python.org/2/library/threading.html
.. _celery: http://www.celeryproject.org/
.. _installing/building Redis server: http://redis.io/download