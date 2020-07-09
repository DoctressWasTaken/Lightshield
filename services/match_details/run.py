from publisher import Publisher
from subscriber import Subscriber
from logic import Worker, Service
from repeat_marker import RepeatMarker

import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == "__main__":
    marker = RepeatMarker()
    asyncio.run(marker.build(
           "CREATE TABLE IF NOT EXISTS match_id("
           "id BIGINT PRIMARY KEY);"))

    publisher = Publisher()
    subscriber = Subscriber(service_name="MD")
    service = Service(url_snippet="match/v4/matches/%s", marker=marker, max_local_buffer=80)

    def shutdown_handler():
        publisher.shutdown()
        subscriber.shutdown()
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    publisher.start()
    subscriber.start()
    asyncio.run(service.run(Worker))

    publisher.join()
    subscriber.join()
