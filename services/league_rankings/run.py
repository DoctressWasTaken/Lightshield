from publisher import Publisher
from logic import Service, Worker
import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == "__main__":
    publisher = Publisher()
    service = Service(
        url_snippet="league-exp/v4/entries/RANKED_SOLO_5x5/%s/%s?page=%s",
        max_local_buffer=500)

    def shutdown_handler():
        publisher.shutdown()
        service.shutdown()

    signal.signal(signal.SIGTERM, shutdown_handler)

    publisher.start()
    asyncio.run(service.run(Worker))

    publisher.join()
