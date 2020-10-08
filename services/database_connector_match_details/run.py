
from subscriber import Subscriber
import signal
import asyncio
import uvloop
from logic import Worker
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


if __name__ == "__main__":
    subscriber = Subscriber(service_name="DCMD")
    dbworker = Worker()

    def shutdown_handler():
        subscriber.shutdown()
        dbworker.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    subscriber.start()
    dbworker.start()

    subscriber.join()
    dbworker.join()
