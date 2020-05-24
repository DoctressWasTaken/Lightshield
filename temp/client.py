import asyncio
import aiohttp
import datetime
import logging

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

delay = []

async def fetch(session, id):
    url = "http://localhost:8888/_"
    url += str(id)
    start = datetime.datetime.now()
    print(10 * "#")
    async with session.get(url) as response:
        print(10 * "#")
        res = await response.text()
        #print(res)
        #print(id)
        diff = -10 + (datetime.datetime.now() - start).total_seconds()

        if diff > 1:
            print(id)
            print("\t", diff)
        delay.append(diff)
        return res

async def main():
    requests = 1

    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(requests):
            tasks.append(
                asyncio.create_task(fetch(session, i)))
            await asyncio.sleep(0.05)
        responses = await asyncio.gather(*tasks)

    print(10 * "#")
    print(sum(delay) / requests)

if __name__ == "__main__":
    asyncio.run(main())
