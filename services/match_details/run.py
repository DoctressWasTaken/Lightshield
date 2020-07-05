from publisher import Publisher
from subscriber import Subscriber
from service import ServiceClass as Service
from logic import Worker
from db_connector import Worker as DBWorker

import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == "__main__":
    subscriber = Subscriber(service_name="MD")
    service = Service(url_snippet="match/v4/matches/%s", max_local_buffer=80)
    db_connector = DBWorker()

    def shutdown_handler():
        subscriber.shutdown()
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)
    db_connector.start()
    subscriber.start()
    asyncio.run(service.run(Worker))

    db_connector.shutdown()  # Delayed shutdown to allow the logic to finish all open requests.
    subscriber.join()
    db_connector.join()
