import websockets
import asyncio
import json


class Websocket:

    def __init__(self, queues, return_dict, base_url, host="localhost",
                 port=6789):
        self.connections = set()
        self.host = host
        self.port = port
        self.base_url = base_url
        self.queues = queues
        self.return_dict = return_dict
        self.rejected = {}

    async def register(self, websocket):
        self.connections.add(websocket)

    async def unregister(self, websocket):
        self.connections.remove(websocket)

    async def listen(self, websocket, remote_address):
        """Listen for new message from the websocket.

        Adds the messages to the queue if theres still space.
        Required Format for incoming messages:
        - JSON Dump
        - Dictionary with the following keys:
            - endpoint, containing the endpoint name only
            - url, containing the partial url starting with / (after /lol)
                -> includes %s placeholder
            - tasks as list of lists with the %s placeholder items in order
                First item in the list:
                - id, containing an id to easier assign responses to tasks
        """
        try:
            message = await asyncio.wait_for(websocket.recv(), timeout=1)
            print("Got a message")
        except asyncio.TimeoutError:
            return
        except Exception as err:
            print(err)
            return
        msg = json.loads(message)
        endpoint = msg['endpoint']
        url = msg['url']
        tasks = msg['tasks']
        print(f"Received {len(tasks)} tasks.")
        try:
            while tasks:
                task = []
                task = tasks.pop()
                id = task.pop(0)
                task_dict = {
                    "origin": remote_address,
                    "target": (self.base_url + url) % tuple(task),
                    "id": id
                }
                if self.queues[endpoint]['max'] > len(
                        self.queues[endpoint]['tasks']):
                    self.queues[endpoint]['tasks'].append(task_dict)
                else:
                    break
            while tasks:
                task = tasks.pop()
                self.rejected[remote_address].append(task.pop(0))
        except Exception as err:
            print("Exception", err)
            raise err

    async def push(self, websocket, remote_address):
        """Push finalized requests back to the websocket user.

        Return format:
        Dictionary keys:
            - status: "rejected" or "done" depending on if it requests were made
            - tasks: List of dicts of the tasks
                - id: The id that was initially assigned
                - status: Response Status Code
                - headers: Response Headers
                - result: Response Body

        """
        # Check for entries in the returning dict
        # Return the entries
        if self.rejected[remote_address]:
            print("Got rejected messages")
            await websocket.send(json.dumps({
                "status": "rejected",
                "tasks": self.rejected[remote_address]}))
            self.rejected[remote_address] = []

        if self.return_dict[remote_address]:
            print("Got done messages")
            await websocket.send(json.dumps({
                "status": "done",
                "tasks": self.return_dict[remote_address]}))
        self.return_dict[remote_address] = []

    async def run(self, websocket, path):
        """Manage websocket connections.

        Delegates actual work to the methods push and listen.
        """
        # register(websocket) sends user_event() to websocket
        name = await websocket.recv()

        remote_address = f"{name} {json.dumps(websocket.remote_address)}"
        print("User registered.", remote_address)

        self.return_dict[remote_address] = []
        self.rejected[remote_address] = []

        try:
            while websocket.open:
                await self.listen(websocket, remote_address)
                await self.push(websocket, remote_address)
                await asyncio.sleep(1)
        except Exception as err:
            print(err)

        finally:
            print("Disconnecting", remote_address)
            del self.return_dict[remote_address]
            del self.rejected[remote_address]

    def stop(self):
        self.stopped.set_result(None)

    async def serve(self):
        self.stopped = asyncio.get_event_loop().create_future()
        async with websockets.serve(self.run, self.host,
                                    self.port, max_size=None):
            await self.stopped
