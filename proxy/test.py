import asyncio
import aiohttp



async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(21):
            tasks.append(
                asyncio.create_task(
                    fetch(
                        session, 
                        f'http://localhost:8888/league-exp/v4/entries/RANKED_SOLO_5x5/DIAMOND/1?page={i}')
                )
            )
            #await asyncio.sleep(0.01)
        responses = await asyncio.gather(*tasks)
        

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
