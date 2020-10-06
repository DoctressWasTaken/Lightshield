"""The Publisher is the output segment of the service. It will broadcast all packages processed.

Each service can limit the output by providing a list of Required Subscriber. As long as not all
required subscriber are connected the publisher will pause.
Additional service can connect without halting/interrupting the publishing (e.g. logging systems).
"""
import os
import logging
import asyncio
import threading
import aioredis
import websockets
from aiohttp import web


class Publisher(threading.Thread):
    """Threaded class that handles the sending of local requests to downstream services.

    Data is taken from an outgoing buffer and broadcasted.
    """

    def __init__(self, host='0.0.0.0', port=9999):
        """Initiate logging and global variables."""
        super().__init__()
        self.required_subs = None
        if 'REQUIRED_SUBSCRIBER' in os.environ and os.environ['REQUIRED_SUBSCRIBER'] != '':
            self.required_subs = os.environ['REQUIRED_SUBSCRIBER'].split(',')
            print("Required subs: %s" % self.required_subs)
        else:
            print("No required subs.")
        self.connection_params = [host, port]
        self.stopped = False
        self.redisc = None

        self.clients = set()
        self.client_names = {}

        self.runner = None  # Async server runner

        self.logging = logging.getLogger("Publisher")
        self.logging.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(
            logging.Formatter('%(asctime)s [Publisher] %(message)s'))
        self.logging.addHandler(handler)

    def stop(self) -> None:
        """Initiate shutdown."""
        self.logging.info("Received shutdown signal. Shutting down.")
        self.stopped = True

    def run(self) -> None:
        """Initiate the async loop/websocket server."""
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        asyncio.run(self.async_run())

    async def init(self) -> None:
        """Initiate redis connection."""
        self.redisc = await aioredis.create_redis_pool(
            ('redis', 6379), db=0, encoding='utf-8')

    async def async_run(self) -> None:
        """Run async initiation and start websocket server."""
        app = web.Application()
        app.add_routes([web.get('', self.handler)])
        await self.init()
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, *self.connection_params)

        shutdown_handler = asyncio.create_task(self.shutdown_handler())
        await site.start()
        self.logging.info("Started server.")
        await shutdown_handler
        self.logging.info("Shutdown server.")

    async def shutdown_handler(self):
        """Set server to exit on shutdown signal."""
        while not self.stopped:
            await asyncio.sleep(1)
        await self.runner.cleanup()

    async def sender(self):
        """Send data to clients as long as all clients are connected.

        Initiated by the first connected client to avoid it running multiple times.
        Once no more clients are connected this terminates itself.
        """
        count = 0
        while all([True if item in self.client_names.keys() else False for item in self.required_subs]):
            task = await self.redisc.lpop('packages')
            if task:
                try:
                    await asyncio.wait([client.send_str(task) for client in self.clients])
                    count += 1
                except BaseException as err:
                    self.logging.info("Exception %s received.", err.__class__.__name__)
                await asyncio.sleep(0.05)
            else:
                await asyncio.sleep(0.5)
        self.logging.info("Sent %s tasks.", count)

    async def handler(self, request) -> None:
        """Handle the websocket client connection."""
        client_name = None
        sender_task = None
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.clients.add(ws)
        count = 0
        try:
            message = await asyncio.wait_for(ws.receive(), timeout=2)
            if not (content := message.data).startswith('ACK'):
                self.logging.info("Received non ACK message: %s", content)
                return ws
            client_name = content.split("_")[1]
            self.client_names[client_name] = True

            if all([True if item in self.client_names.keys() else False for item in self.required_subs]):
                sender_task = asyncio.create_task(self.sender())

            while not ws.closed:
                await asyncio.sleep(0.5)

        except asyncio.TimeoutError:
            return ws
        finally:
            del self.client_names[client_name]
            if sender_task:
                await sender_task
            self.clients.remove(ws)
            await ws.close()
