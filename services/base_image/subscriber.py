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
import aiohttp


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

    async def async_run(self) -> None:
        """Initiate and Start the websocket client."""
        await self.init()
        while not self.stopped:
            while not self.stopped and await self.redisc.llen('tasks') > 0.3 * self.max_buffer:
                await asyncio.sleep(0.5)

            async with aiohttp.ClientSession() as session:
                await self.runner(session)

    async def runner(self, session) -> None:
        async with session.ws_connect(self.uri) as ws:
            self.logging.info("Connected to provider.")
            await ws.send_str("ACK_" + self.service_name)

            count = 0
            while not self.stopped:
                try:
                    message = await asyncio.wait_for(ws.receive(), timeout=2)
                    self.logging.info("Message type %s", message.type)
                    self.logging.info(message.data)
                    content = json.loads(message.data)
                    count += 1
                    if await self.redisc.lpush('tasks', json.dumps(content)) >= self.max_buffer:
                        await ws.close()
                        break
                except asyncio.TimeoutError:
                    await asyncio.sleep(1)

            self.logging.info("Received %s tasks", count)
