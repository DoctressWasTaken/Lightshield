from lol_dto import (
    Base, Summoner, Match, Team, Player, Runes)
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists


class PermanentDB:
    base_url = "postgresql://postgres@postgres/raw"

    def __init__(self):
        self.engine = None
        engine = create_engine(self.base_url)
        if not database_exists(engine.url):
            print("Created db")
            create_database(engine.url)
        Base.metadata.create_all(engine)

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


 INSERT INTO summoner
processor_summoner_1  |                     (account_id, puuid, rank, wins, losses)
processor_summoner_1  |                     VALUES ('11AYWpXoVt7Td5bBFH-2HBmFJGXxGW97mLLAdXQpnXPI6LE', 'SIpNXr-dK454bBh9REMdd9PmerXbah4DiqECcVdvoLT5a2QJOVpWfJmRvug61WQW8MvXrwxrur7Gbw', 1600, 17, 22)
processor_summoner_1  |                     ON CONFLICT (puuid)
processor_summoner_1  |                     DO
processor_summoner_1  |                         UPDATE SET rank = EXCLUDED.rank,
processor_summoner_1  |                                    wins = EXCLUDED.wins,
processor_summoner_1  |                                    losses = EXCLUDED.losses
processor_summoner_1  |                     ;
