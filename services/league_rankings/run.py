from logic import Service
import signal
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == "__main__":

    service = Service()

    def shutdown_handler():
        service.shutdown()
    signal.signal(signal.SIGTERM, shutdown_handler)

    asyncio.run(service.run())

