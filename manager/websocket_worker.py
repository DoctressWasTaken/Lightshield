import websockets
import asyncio
import json


class Worker:

    def __init__(self, server, rabbitmq, postgres):
        self.server = server
        self.rabbitmq = rabbitmq
        self.postgres = postgres
        self.clients = set()

    async def start(self):
        process = asyncio.create_task(self.run())
        server = await websockets.serve(self.websocket, "0.0.0.0", 8765)
        await process
        await server.wait_closed()

    async def run(self):

        while True:
            await asyncio.sleep(5)
            tick_data = {
                "database_sizes": {},
                "queues": None
            }
            resp = self.rabbitmq.cmd(
                'rabbitmqctl list_queues --formatter json')
            tick_data['queues'] = json.loads(
                resp.output.decode('utf-8').replace('\n', ''))

            for server in self.server:

                tick_data['database_sizes'][server] = await self.server[
                    server].get_stats()
            if self.clients:
                await asyncio.wait(
                    [client.send(json.dumps(tick_data)) for client in
                     self.clients])

    async def websocket(self, websocket, path):
        self.clients.add(websocket)
        name = await websocket.recv()
        print(f"< {name}")
        try:
            async for message in websocket:
                print(message)
        finally:
            self.clients.remove(websocket)
            print("Exiting")
