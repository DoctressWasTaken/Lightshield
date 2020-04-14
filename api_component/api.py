import json
from templates import DefaultEndpoint, Limit
import aiohttp

class API:

    def __init__(self, server, key):
        print(f"Initiating API for {server}")
        self.server = server.lower()
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

    async def request(self, endpoint: str, method: str, params: dict):
        for limit in self.application_limits:
            if limit.is_blocked():
                raise Exception("Blocked")
        return await getattr(self, endpoint).request(self.application_limits, method, params)

    async def send(self, url):
        url = "https://" + self.server + ".api.riotgames.com/lol/" + url
        print(f"Requesting url {url}")
        async with aiohttp.ClientSession() as session:
            headers = {'X-Riot-Token': self.key}
            async with session.get(url, headers=headers) as response:
                resp = await response.json()
                return response

    class League(DefaultEndpoint):
        """League-V4 Endpoints"""
        url = 'league/v4/'
        methods = {
            'entries': {
                'params': ['queue', 'tier', 'division', 'page'],
                'url': 'entries/%s/%s/%s?page=%s',
                'allowed_codes': [200]
            }
        }

    class LeagueExperimental(DefaultEndpoint):
        """League-EXP-V4 Endpoints"""
