import json
from templates import DefaultEndpoint, Limit
import aiohttp
import asyncio

class API:

    def __init__(self, server, key):
        """Initiate the limits and endpoints for the api.

        ::param::server::The server the api is referencing to
        ::param::key::The api key used to request data
        """
        print(f"Initiating API for {server}")
        self.server = server
        self.key = key
        limits = json.loads(open("limits.json").read())
        self.application_limits = []

        print("\tInitiating Application Rate limits")
        for limit in limits['APP']:
            self.application_limits.append(
                Limit(span=int(limit), max=limits['APP'][limit])
            )
        self.league = self.League(limits['METHODS']['league'], self)
        self.league_exp = self.LeagueExperimental(limits['METHODS']['league-exp'], self)
        self.summoner = self.Summoner(limits['METHODS']['summoner'], self)

    async def request(self, endpoint: str, method: str, params: dict):
        for limit in self.application_limits:
            if limit.is_blocked():
                raise Exception("Blocked")
        return await getattr(self, endpoint).request(self.application_limits, method, params)

    async def run(self, connection):
        endpoints = [
            asyncio.create_task(self.league.run(connection)),
            asyncio.create_task(self.league_exp.run(connection)),
            asyncio.create_task(self.summoner.run(connection))]
        await asyncio.gather(*endpoints)
        return

    async def send(self, url):
        url = "https://" + self.server + ".api.riotgames.com/lol/" + url
        #print(f"Requesting url {url}")
        headers = {'X-Riot-Token': self.key}
        async with aiohttp.request('GET', url, headers=headers) as response:
            data = await response.json()
            print(data)
            headers = response.headers
            status = response.status
            if response.status == 429:
                print(response.headers)
            return data, headers, status

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                print(data)
                headers = response.headers
                status = response.status
                if response.status == 429:
                    print(response.headers)
                return data, headers, status


    class League(DefaultEndpoint):
        """League-V4 Endpoints"""
        url = 'league/v4/'
        name = "LEAGUE"
        methods = {
            'entries': {
                'params': ['queue', 'tier', 'division', 'page'],
                'url': 'entries/%s/%s/%s?page=%s',
                'allowed_codes': [200]
            }
        }

    class LeagueExperimental(DefaultEndpoint):
        """League-EXP-V4 Endpoints"""
        name = "LEAGUE-EXP"
        url = 'league-exp/v4/'
        methods = {
            'entries': {
                'params': ['queue', 'tier', 'division', 'page'],
                'url': 'entries/%s/%s/%s?page=%s',
                'allowed_codes': [200]
            }
        }

    class Summoner(DefaultEndpoint):
        """Summoner-V4 Endpoints."""
        name = "SUMMONER"
        url = "summoner/v4/"
        methods = {
            'summonerId': {
                'params': ['summonerId'],
                'url': 'summoners/%s',
                'allowed_codes': [200]
            }
        }