import asyncio
import aiohttp



async def fetch(session, url):
    async with session.get(url) as response:
        await response.text()
        return response.status

async def main():
    async with aiohttp.ClientSession() as session:
        for rounds in range(5):
            tasks = []
            tasks.append(
                asyncio.sleep(3)
            )
            for i in range(1, 11):
                tasks.append(
                    asyncio.create_task(
                        fetch(
                            session,
                            f'http://localhost:8888/league-exp/v4/entries/RANKED_SOLO_5x5/DIAMOND/I?page={i}')
                    )
                )
                await asyncio.sleep(0.01)
            responses = await asyncio.gather(*tasks)
            responses.pop(0)
            for resp in responses:
                print(resp)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
