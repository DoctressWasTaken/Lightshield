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
        self.logging = logging.getLogger("Subscriber")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Subscriber] %(message)s'))
        self.logging.addHandler(handler)

        self.uri = "ws://%s:%s" % (host, port)
        self.service_name = service_name
        self.stopped = False
        self.redisc = None
        self.max_buffer = int(os.environ['MAX_TASK_BUFFER'])

        self.connected_to_publisher = False
        self.received_packages = 0

        self.logging.info("Initiated subscriber.")

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
        logger = asyncio.create_task(self.logger())
        while not self.stopped:
            while not self.stopped and (
                    await self.redisc.llen('tasks') > 0.3 * self.max_buffer
                    or await self.redisc.llen('packages') > 500):
                await asyncio.sleep(0.5)
            if self.stopped:
                break

            try:
                async with aiohttp.ClientSession() as session:
                    await self.runner(session)
            except Exception as err:
                self.logging.info(err)
                await asyncio.sleep(5)
        await logger

    async def logger(self) -> None:
        """Handle passive logging tasks."""
        while not self.stopped:
            await asyncio.sleep(15)
            self.logging.info(
                "Received packages: %s | Currently buffered input packages: %s/%s. Connection to publisher: %s",
                self.received_packages,
                await self.redisc.llen('tasks'),
                self.max_buffer,
                self.connected_to_publisher)
            self.received_packages = 0

    async def runner(self, session) -> None:
        self.logging.info("Establishing connection to publisher.")
        async with session.ws_connect(self.uri) as ws:
            try:
                await ws.send_str("ACK_" + self.service_name)
                self.connected_to_publisher = True
                while not self.stopped:

                    try:
                        message = await asyncio.wait_for(ws.receive(), timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    if message.type == aiohttp.WSMsgType.TEXT:
                        await self.redisc.lpush('tasks', message.data)
                        self.received_packages += 1
                    elif message.type == aiohttp.WSMsgType.CLOSED:
                        return
                    elif message.type == aiohttp.WSMsgType.CLOSE:
                        return
                    else:
                        print(message.type)
                    if await self.redisc.llen('tasks') > self.max_buffer \
                            or await self.redisc.llen('packages') > 500:
                        return
            except asyncio.TimeoutError:
                return
            finally:
                self.connected_to_publisher = False
        self.logging.info("Closed connection to publisher.")
