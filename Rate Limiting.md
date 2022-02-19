# Rate Limiting

Lightshield offers a multi-host ratelimiting solution by syncing limits through a redis server.

Ratelimits are automatically generated and depend on the user to provide the proper server/method.

### How does it work

A user initiates an endpoint by providing 2 values:
- Server
- Zone

E.g. `server=EUW1, zone='match_details'`

Ratelimits are then checked as follows:
- Server -> App-Rate-Limit
- Zone -> Method-Rate-Limit

Naming schema for each zone is up to the user as the individual endpoints are not predefined.


### Usage

```python
from lightshield.proxy import Proxy
import aiohttp
import asyncio

async def run(proxy):
    # Initiate the redis connector in async context
    await proxy.init(host='localhost', port=5432)
    
    # Create an endpoint for your requests
    zone = await proxy.get_endpoint(server='europe', zone='league-exp')
    
    async with aiohttp.ClientSession(headers={'X-Riot-Token': ''}) as session:
        for page in range(1, 10):
            # Pass request url + session to the API
            zone.request('https://euw1.api.riotgames.com/lol/league-exp/v4/entries/RANKED_SOLO_5x5/SILVER/I?page=%s' % page, session)

def main():
    # Set up the proxy instance
    proxy = Proxy()
    asyncio.run(run(proxy))

if __name__ == "__main__":
    main()
```


### Sourcecode
See [here](lightshield/proxy)
