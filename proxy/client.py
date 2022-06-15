import aiohttp
import asyncio
import yaml


async def main():
    async with aiohttp.ClientSession() as session:
        url = "http://euw.riotgames.com/tft/summoner/v1/summoners/by-account/encryptedAccountId"
        async with session.get(url, proxy='http://172.20.0.3:8301') as resp:
            txt = await resp.text()
            print(txt)
        with open('endpoints.yaml') as endpoints:
            ep = yaml.safe_load(endpoints)['endpoints']
            for cat in ep:
                for endpoint in ep[cat]:
                    url = "http://euw.riotgames.com" + endpoint.replace('{', '').replace('}', '')
                    print(url)
                    async with session.get(url, proxy='http://172.20.0.3:8301') as resp:
                        print(resp)


asyncio.run(main())
