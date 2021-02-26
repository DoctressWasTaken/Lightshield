"""The run.py file is to be called by the startup script.

It initializes all parts of the base service that are to be used by the inheriting service.
E.g. If Data is not to be passed on further, but the service instead poses as a database
    connector, no publisher is required.
    If a service receives data not from another service but from a db or via calls, no subscriber
    is needed.
"""

import asyncio
import signal

import uvloop
from logic import Service, Worker
from publisher import Publisher
from repeat_marker import RepeatMarker
from subscriber import Subscriber

uvloop.install()

if __name__ == "__main__":
    marker = RepeatMarker()
    await asyncio.run(marker.build(""))

    publisher = Publisher()
    subscriber = Subscriber(service_name="")
    service = Service(url_snippet="", marker=marker)

    def shutdown_handler():
        """Shutdown handler called by the termination signal manager.

        Sets the stopping flag in each element which will force a shutdown of each section.
        """
        publisher.shutdown()
        subscriber.shutdown()
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    publisher.start()
    subscriber.start()
    asyncio.run(service.run(Worker))

    publisher.join()
    subscriber.join()
