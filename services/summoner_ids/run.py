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
           "CREATE TABLE IF NOT EXISTS summoner_ids("
           "summonerId TEXT PRIMARY KEY,"
           "accountId TEXT,"
           "puuid TEXT);"))

    publisher = Publisher()
    subscriber = Subscriber(service_name="SI")
    service = Service(url_snippet="summoner/v4/summoners/%s", marker=marker)

    def shutdown_handler():
        """Shutdown."""
        publisher.shutdown()
        subscriber.shutdown()
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    publisher.start()
    subscriber.start()
    asyncio.run(service.run(Worker))

    publisher.join()
    subscriber.join()
