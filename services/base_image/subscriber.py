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
        self.max_buffer = os.environ['MAX_TASK_BUFFER']

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

    async def pause_handler(self, websocket) -> None:
        """Handle the pausing and unpausing commands to the parent services."""
        await websocket.send("PAUSE")
        while not self.stopped and await self.redisc.llen('tasks') > self.max_buffer:
            await asyncio.sleep(0.5)
        if self.stopped:
            return
        await websocket.send("UNPAUSE")

    async def async_run(self) -> None:
        """Initiate and Start the websocket client."""
        await self.init()
        while not self.stopped:
            async with websockets.connect(self.uri) as websocket:
                await websocket.send("ACK_" + self.service_name)
                while not self.stopped:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=2)
                        content = json.loads(message)
                    except asyncio.TimeoutError:
                        continue
                    except json.JSONDecodeError:
                        continue
                    length = self.redisc.lpush('tasks', json.dumps(content))
                    if length >= self.max_buffer:
                        await self.pause_handler(websocket=websocket)





