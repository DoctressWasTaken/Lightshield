"""The Subscriber service provides the input module for a service.

By linking a container to this container using the alias 'provider' this service will
attempt to connect to the publishing services websocket on (default) ws://provider:9999.
The service will authenticate using the passed on service name.
"""
import os
import json
import asyncio
import logging
import threading
import aioredis
import websockets


class Subscriber(threading.Thread):
    """Threaded class that handles the retrieval and buffering of requests to this service.

    Data received is saved in the redis database.
    """

    def __init__(self, service_name, host='provider', port=9999):
        """Initiate logging and global variables."""
        super().__init__()
        self.uri = "ws://%s:%s" % (host, port)
        self.service_name = service_name
        self.stopped = False
        self.redisc = None
        self.max_buffer = int(os.environ['MAX_TASK_BUFFER'])

        self.logging = logging.getLogger("Subscriber")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

    def run(self) -> None:
        """Initiate the async loop."""
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.async_run())

    def stop(self) -> None:
        """Initiate shutdown."""
        self.logging.info("Received shutdown signal. Shutting down.")
        self.stopped = True

    async def init(self) -> None:
        """Initiate async elements."""
        self.redisc = await aioredis.create_redis_pool(
            ('redis', 6379), db=0, encoding='utf-8')

    async def runner(self) -> None:
        try:
            self.logging.info("Connecting to provider.")
            async with websockets.connect(self.uri) as websocket:
                await websocket.send("ACK_" + self.service_name)
                while not self.stopped:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=10)
                        content = json.loads(message)
                    except asyncio.TimeoutError:
                        continue
                    except json.JSONDecodeError:
                        continue
                    length = await self.redisc.lpush('tasks', json.dumps(content))
                    if length >= self.max_buffer:
                        return

        except websockets.exceptions.InvalidHandshake:
            self.logging.info("Could not establish connection, handshake failed.")
            await asyncio.sleep(1)
        except BaseException as err:  # pylint: disable=broad-except
            self.logging.info("Connection broke. Resetting. [%s]", err.__class__.__name__)
            #raise err
            await asyncio.sleep(1)
        

    async def async_run(self) -> None:
        """Initiate and Start the websocket client."""
        await self.init()
        while not self.stopped:
            while not self.stopped and await self.redisc.llen('tasks') > 0.3 * self.max_buffer:
                await asyncio.sleep(0.5)
            await self.runner()

