import threading
import json
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta
import pytz
import logging

from _app import LimitBlocked


class Endpoint(threading.Thread):

    def __init__(self, url, limits, app, loop, return_dict, headers):
        super().__init__()
        self.url = url

        self.logging = logging.getLogger(self.url)
        self.logging.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(
            logging.Formatter(f'%(asctime)s [{self.url}] %(message)s'))
        self.logging.addHandler(ch)
        self.app = app  # Central app rate limiter
        self.is_interrupted = False
        self.return_dict = return_dict
        self.loop = loop
        self.headers = headers
        self.queue = None

        # Rate limiting details
        self.max = int(limits.split(":")[1])  # Max requests per bucket
        self.span = int(limits.split(":")[0])  # Bucket duration
        self.call_delay = (
                                      self.span / 2) / self.max  # How quickly requests have to be sent out
        self.end = None  # Datetime of when the bucket will reset
        self.end_type = None  # Can be "verified" or "estimate"

        self.planned_tasks = []
        self.logging.info(
            f"Started {self.url} with limit {self.max} over {self.span}.")

    def stop(self):
        """Stop the thread.

        Called centrally when an interrupt signal is received.
        """
        self.logging.info("Received stop.")
        self.is_interrupted = True

    def init_queue(self):
        """Create a dictionary containing a list for tasks.

        Dictionary to have a second key with a max length.
        """
        if not self.queue:
            max = None
            if self.span < 10:
                max = 10 / self.span * self.max
            else:
                max = self.max * 2
            self.queue = {
                'max': max,
                'tasks': []
            }
        return self.queue

    def run(self):
        """Run method of the thread. calls async run."""
        asyncio.set_event_loop(self.loop)
        asyncio.run(self.async_run())

    async def fetch(self, task, session):
        async with session.get(task['target'],
                               headers=self.headers) as response:
            body = await response.json()
            return {
                "origin": task['origin'],
                "id": task['id'],
                "task": task,
                "body": body,
                "status": response.status,
                "headers": response.headers
            }

    async def return_data(self, result):
        status = result['status']
        if result['origin'] not in self.return_dict:
            self.logging.info(
                "Skipping return because the client disconnected.")
        elif status == 200:  # 200 Success
            self.return_dict[result['origin']].append({
                "id": result['id'],
                "status": 200,
                "headers": dict(result['headers']),
                "result": result['body']
            })
        elif 400 <= status < 500:  # 400 Errors: Returned
            self.logging.info(result)
            self.return_dict[result['origin']].append({
                "id": result['id'],
                "status": status,
                "headers": dict(result['headers']),
                "result": result['body']
            })
        elif status >= 500:  # 500 Errors: Retried
            self.logging.info(result)
            self.queue['tasks'].append(result['task'])

        if "Date" in result['headers']:
            naive = datetime.strptime(
                result['headers']['Date'],
                '%a, %d %b %Y %H:%M:%S GMT')
            return pytz.timezone('GMT') \
                .localize(naive, is_dst=None) \
                .astimezone(pytz.utc)
        return None

    async def worker(self):
        # Create a local list of tasks to request during next API window
        while len(self.planned_tasks) < self.max:
            try:
                self.planned_tasks.append(self.queue['tasks'].pop())
            except IndexError:
                break

        if not self.planned_tasks:
            return
        # Wait, till the endpoint unlocks
        wait = False
        while self.end and datetime.now(timezone.utc) < self.end:
            await asyncio.sleep(0.1)
            if not wait:
                self.logging.info("Waiting for endpoint to unlock.")
                wait = True
            if self.is_interrupted:
                return
        # Wait, till the app unlocks
        wait = False
        while not self.app.open:
            await asyncio.sleep(0.1)
            if not wait:
                self.logging.info("Waiting for app to unlock.")
                wait = True
            if self.is_interrupted:
                return
        self.logging.info("Starting call.")
        async with aiohttp.ClientSession() as session:
            self.end = datetime.now(timezone.utc)
            self.end_type = "estimate"
            calls = []
            while self.planned_tasks:
                if self.is_interrupted:
                    return
                try:
                    await asyncio.get_event_loop()\
                        .run_in_executor(None, self.app.register)
                except LimitBlocked:
                    break
                task = self.planned_tasks.pop()
                calls.append(
                    asyncio.create_task(self.fetch(task, session)))
                await asyncio.sleep(self.call_delay)
            results = await asyncio.gather(*calls)
            self.logging.info(f"Made {len(results)} calls.")
        earliest = datetime.now(timezone.utc)
        for res in results:
            date = await self.return_data(res)
            if date and date < earliest:
                earliest = date
        self.end = earliest + timedelta(seconds=self.span + 0.5)
        self.end_type = "verified"

    async def live_check(self):
        """Periodic output to show the thread is still running."""
        while not self.is_interrupted:
            self.logging.info(f"Thread {self.url} is alive.")
            for i in range(30):
                await asyncio.sleep(2)
                if self.is_interrupted:
                    break

    async def async_run(self):
        live = asyncio.create_task(self.live_check())
        while not self.is_interrupted:
            await self.worker()
            await asyncio.sleep(0.1)
        await live
        self.logging.info("Exited Thread.")
    # All requests should be done in the first half of the Endpoint Time bucket
