#!/usr/bin/env python

import asyncio
import signal

import websockets

class Server:

    def __init__(self):
        pass

    async def run(self):
        loop = asyncio.get_running_loop()
        stop = loop.create_future()
        loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)
        print("Turning on websocket")
        async with websockets.serve(self.serve, "0.0.0.0", 8765):
            await stop

    async def serve(self, websocket):
        name = await websocket.recv()
        print(f"<<< {name}")


server = Server()

asyncio.run(server.run())
