import asyncio
from aio_pika import connect_robust, Message, DeliveryMode
import logging


class QueueHandler:
    connection = channel = None
    is_shutdown = False

    def __init__(self, queue_name, user=None, password=None, host=None, port=None):
        self.queue = queue_name
        self.logging = logging.getLogger(queue_name)

        if user and password and host and port:
            self.connect_string = "amqp://%s:%s@%s:%s" % (user, password, host, port)

    def shutdown(self) -> None:
        self.is_shutdown = True

    async def init(self, durable=False, connection=None) -> None:
        if not self.connect_string and not connection:
            self.logging.error("No connection was made available.")
            exit()
        if connection:
            self.connection = connection
        else:
            self.connection = await connect_robust(self.connect_string)
        self.channel = self.connection.channel()
        await self.channel.declare_queue(self.queue, durable=durable)

    async def wait_threshold(self, threshold) -> int:
        """Blocking loop until the queue has reached a lower threshold."""
        self.logging.info("Blocking till queue is at %s.", threshold)
        while not self.is_shutdown:
            await asyncio.sleep(2)
            count = (
                await self.channel.declare_queue(self.queue, passive=True)
            ).declaration_result.message_count
            if count <= threshold:
                return count

    async def send_tasks(self, tasks, persistent=True):
        """Insert tasks into the queue.

        Tasks have to be prepared as a byte like object.
        This can mean either encoding or pickle.
        """
        self.logging.info("Adding %s new tasks.", len(tasks))
        delivery_mode = (
            DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT
        )
        for task in tasks:
            await self.channel.default_exchange.publish(
                Message(task, delivery_mode=delivery_mode), routing_key=self.queue
            )