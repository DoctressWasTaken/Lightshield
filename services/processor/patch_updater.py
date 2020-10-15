"""Pull the patch timings + server delays from cdragons patches file:

https://raw.githubusercontent.com/CommunityDragon/Data/master/patches.json

"""
import asyncio
import aiohttp
import json


async def get():
    """Pull the patches data from the server and filter to relevant data."""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://raw.githubusercontent.com/CommunityDragon/Data/master/patches.json") as response:
            data = await response.json(content_type=None)
    timestamps = sorted([el['start'] for el in data['patches']], reverse=True)[:5]
    patches = {el['start']: el for el in data['patches'] if el['start'] in timestamps}
    select = {timestamp: patches[timestamp] for timestamp in timestamps}

    return patches