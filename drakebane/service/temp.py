#!/usr/bin/env python

# WS client example

import asyncio
import websockets


async def hello():
    uri = "ws://10.98.250.14:30000"
    async with websockets.connect(uri) as websocket:
        name = input("What's your name? ")
        await websocket.send(name)
        print(f">>> {name}")


asyncio.run(hello())
