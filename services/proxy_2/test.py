import websockets

#!/usr/bin/env python

import asyncio
import websockets
import json

async def hello():
    uri = "ws://localhost:6789"
    async with websockets.connect(uri) as websocket:
        await asyncio.sleep(0.5)
        content = {
            "endpoint": "league-exp",
            "url": "/league-exp/v4/entries/%s/%s/%s?page=%s",
            "tasks": [
                ["RANKED_SOLO_5x5", "CHALLENGER", "I", 1],
                ["RANKED_SOLO_5x5", "CHALLENGER", "I", 2],
                ["RANKED_SOLO_5x5", "CHALLENGER", "I", 3]
            ]
        }
        remaining = 3
        await websocket.send(json.dumps(content))
        while remaining > 0:
            await asyncio.sleep(0.1)
            msg = await websocket.recv()
            print(msg)
            data = json.loads(msg)
            print(data["result"])
            tasks = data["tasks"]
            for task in tasks:
                remaining -= 1
                print(len(task["result"]))
                print(task['headers'])
asyncio.get_event_loop().run_until_complete(hello())
