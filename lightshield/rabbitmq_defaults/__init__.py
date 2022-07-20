import asyncio
from aio_pika import connect_robust, Message, DeliveryMode
import logging
from functools import partial


class QueueHandler:
    connection = channel = None
    is_shutdown = False
    connect_string = None

    def __init__(self, queue_name, user=None, password=None, host=None, port=None):
        self.queue = queue_name
        self.logging = logging.getLogger(queue_name)

        if user and password and host and port:
            self.connect_string = "amqp://%s:%s@%s:%s" % (user, password, host, port)

    def shutdown(self) -> None:
        self.is_shutdown = True

    async def init(self, durable=False, prefetch_count=100, connection=None) -> None:
        if not self.connect_string and not connection:
            self.logging.error("No connection was made available.")
            exit()
        if connection:
            self.connection = connection
        else:
            self.connection = await connect_robust(self.connect_string)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=prefetch_count)
        await self.channel.declare_queue(self.queue, durable=durable)

    async def wait_threshold(self, threshold) -> int:
        """Blocking loop until the queue has reached a lower threshold."""
        while not self.is_shutdown:
            await asyncio.sleep(2)
            count = (
                await self.channel.declare_queue(self.queue, passive=True)
            ).declaration_result.message_count
            if count <= threshold:
                self.logging.info("Queue reached threshold of below %s.", threshold)
                return count

    async def send_tasks(self, tasks: list[bin], persistent=True):
        """Insert tasks into the queue.

        Tasks have to be prepared as a byte like object.
        This can mean either encoding or pickle.
        """
        delivery_mode = (
            DeliveryMode.PERSISTENT if persistent else DeliveryMode.NOT_PERSISTENT
        )
        for task in tasks:
            await self.channel.default_exchange.publish(
                Message(task, delivery_mode=delivery_mode), routing_key=self.queue
            )

    async def consume_tasks(self, func, arguments=None):
        """Start a consumer and return the cancel task."""
        if arguments is None:
            arguments = {}
        queue = await self.channel.declare_queue(self.queue, passive=True)
        tag = await queue.consume(partial(func, **arguments))
        return partial(queue.cancel, tag)
