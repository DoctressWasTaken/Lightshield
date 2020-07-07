from publisher import Publisher
from subscriber import Subscriber
from logic import Worker, Service
import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == "__main__":
    publisher = Publisher()
    subscriber = Subscriber(service_name="MH")
    service = Service(
        url_snippet="match/v4/matchlists/by-account/%s?beginIndex=%s&endIndex=%s&queue=420")


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
