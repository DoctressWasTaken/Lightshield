from lol_dto import (
    Base, Summoner, Match, Team, Player, Runes)
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy_utils import create_database, database_exists

class PermanentDB:

    base_url = "postgresql+asyncpg://postgres@postgres/raw"

    def __init__(self):
        self.engine = None
        if not database_exists(self.base_url):
            create_database(self.base_url)

    async def create_db(self):
        self.engine = create_async_engine(self.base_url, echo=True)
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    def commit_db(self, patch):
        if patch in self.engines:
            self.engines[patch]['session'].commit()


    def add_match(self, patch, match):
        if patch not in self.engines:
            self.create_db(patch)
        session = self.engines[patch]['sessionmaker']()
        session.add(Match.create(match))
        session.flush()
        session.add_all([
            Team.create(match, side=0),
            Team.create(match, side=1)
        ])
        session.commit()
        player, runes = self.add_player_data(match)
        session.bulk_save_objects(player)
        session.bulk_save_objects(runes)
        session.close()

    def add_player_data(self, match):
        matchId = match['gameId']
        playerObjects = []
        runeObjects = []
        for i in range(1, 11):
            id = [pt for pt in match['participantIdentities'] if pt["participantId"] == i][0]['player']
            data = [pt for pt in match['participants'] if pt["participantId"] == i][0]
            playerObjects.append(Player.create(id, data, matchId, i))
            runeObjects += Runes.create(data, matchId, i)

        return playerObjects, runeObjects
