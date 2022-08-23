import asyncio
import logging
import math


class Buffer:
    def __init__(self, identifier, block_size=500, blocks=16):
        self.recent_tasks = []
        self.in_queue = []
        self.block_size = block_size
        self.blocks = blocks
        self.queue_size = self.block_size * self.blocks
        self.buffer_size = self.queue_size
        self.logging = logging.getLogger("%s | Buffer" % identifier.ljust(5))

    def verify_tasks(self, tasks):
        """Verify a list of tasks and add only tasks that are not included in the buffer to the queue.

        Limits the returned list to how many can fit in the assumed queue.
        returns the list of added tasks to be added to rabbitmq
        """
        added_tasks = []
        for task in tasks:
            if task not in self.recent_tasks and task not in self.in_queue:
                self.in_queue.append(task)
                added_tasks.append(task)
            if len(self.in_queue) >= self.queue_size:
                break
        self.logging.debug("Refilling %s tasks.", len(added_tasks))
        return added_tasks

    def needs_refill(self, task_count=0):
        """Update the in_queue list depending on how many remaining tasks are still found, returns True if a full block is missing."""
        blocks_missing = self.blocks - math.ceil(task_count // self.block_size)

        if blocks_missing > 0:
            self.logging.debug("Found %s blocks to be missing", blocks_missing)
            self.recent_tasks = self.in_queue[: blocks_missing * self.block_size]
            self.buffer_size = self.queue_size + len(self.recent_tasks)
            self.in_queue = self.in_queue[blocks_missing * self.block_size :]
            return True
        return False

    def get_refill_count(self):
        """Return the number to which should be waited before refill."""
        return (self.blocks - 1) * self.block_size
