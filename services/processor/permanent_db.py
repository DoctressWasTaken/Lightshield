from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from lol_dto import (
    Base, Summoner, Match, Team, Player, Runes)
import threading


class PermanentDB:

    base_url = "postgres://postgres@postgres/patch_%s"

    def __init__(self):
        self.engines = {}

    def create_db(self, patch):
        engine = create_engine(self.base_url % patch)
        if not database_exists(engine.url):
            print("Created db for patch %s" % patch)
            create_database(engine.url)
        Base.metadata.create_all(engine)
        self.engines[patch] = {
            "engine": engine,
            "sessionmaker": sessionmaker(bind=engine),
            "session": sessionmaker(bind=engine)()
        }

    def commit_db(self, patch):
        if patch in self.engines:
            self.engines[patch]['session'].commit()

    def add_summoner(self, patch, accountId, puuid, rank, wins, losses):
        if patch not in self.engines:
            self.create_db(patch)
        session = self.engines[patch]['session']

        dBSummoner = session.query(Summoner) \
            .filter_by(accountId=accountId) \
            .first()
        if dBSummoner:
            dBSummoner.rank.append(rank)
            dBSummoner.wins = wins
            dBSummoner.losses = losses
        else:
            new_summoner = Summoner(
                rank=rank,
                wins=wins,
                losses=losses,
                puuid=puuid,
                accountId=accountId,
            )
            session.add(new_summoner)
        session.flush()

    def add_match(self, patch, match):
        if patch not in self.engines:
            self.create_db(patch)
        session = self.engines[patch]['sessionmaker']()
        session.add_all([
            Match.create(match),
            Team.create(match, side=0),
            Team.create(match, side=1)
        ])
        player, runes = self.add_player_data(match)
        session.bulk_save_objects(player)
        session.bulk_save_objects(runes)
        session.commit()

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
