from datetime import datetime, timedelta


async def parse_details(data, matchId, platform):
    """Parse the retrieved data from the Riot API.

    Returns a packaged dataset that will either mark the game as found (with attributes) or missing (404).
    """
    if data["info"]["queueId"] == 0:
        return {"found": False,
                "platform": platform,
                "data": {
                    "matchId": matchId
                }}

    queue = data["info"]["queueId"]
    creation = datetime.fromtimestamp(data["info"]["gameCreation"] // 1000)
    patch = ".".join(data["info"]["gameVersion"].split(".")[:2])
    if (
            "gameStartTimestamp" in data["info"]
            and "gameEndTimestamp" in data["info"]
    ):
        game_duration = (
                data["info"]["gameEndTimestamp"]
                - data["info"]["gameStartTimestamp"]
        )
    else:
        game_duration = data["info"]["gameDuration"]
    if game_duration >= 30000:
        game_duration //= 1000
    win = (data["info"]["teams"][0]["teamId"] == 100) == (
        not data["info"]["teams"][0]["win"]
    )

    day = creation.strftime("%Y_%m_%d")
    patch_int = int("".join([el.zfill(2) for el in patch.split(".")]))
    return {"found": True,
            "platform": platform,
            "data": {
                "queue": queue,
                "creation": creation,
                "patch": patch_int,
                "duration": game_duration,
                "win": win,
                "matchId": matchId
            }}


async def parse_summoners(data, platform):
    """Parse the retrieved data from the Riot API.

    Returns a packaged dataset that will either mark the game as found (with attributes) or missing (404).
    """
    if (
            "gameStartTimestamp" in data["info"]
            and "gameEndTimestamp" in data["info"]
    ):
        game_duration = (
                data["info"]["gameEndTimestamp"]
                - data["info"]["gameStartTimestamp"]
        )
    else:
        game_duration = data["info"]["gameDuration"]
    if game_duration >= 30000:
        game_duration //= 1000
    creation = datetime.fromtimestamp(data["info"]["gameCreation"] // 1000)

    last_activity = creation + timedelta(seconds=game_duration)
    return {
        "last_activity": last_activity,
        "platform": platform,
        "summoners": [
            {"name": player["summonerName"],
             "puuid": player["puuid"]}
            for player in data["info"]["participants"]
        ]
    }
