from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from lol_dto import (
    Base, Summoner, Match, Team, Player, Runes)
import threading


class PermanentDB:

    base_url = "postgres://postgres@longterm/patch_%s"

    def __init__(self):
        self.engines = {}

    def create_db(self, patch):

        if patch not in self.engines:
            engine = create_engine(self.base_url % patch)
            if not database_exists(engine.url):
                print("Created db for patch %s" % patch)
                create_database(engine.url)
            Base.metadata.create_all(engine)
            self.engines[patch] = {
                "engine": engine,
                "sessionmaker": sessionmaker(bind=engine)
            }
        return self.engines[patch]

    def create_session(self, patch):
        self.engines[patch]['session'] = self.engines[patch]['sessionmaker']()

    def close_session(self, patch):
        self.engines[patch]['session'].close()
        del self.engines[patch]['session']

    def add_summoner(self, patch, summoner_dict, matches):
        session = self.engines[patch]['session']
        new_summoners = []
        for match in matches:
            for player in match.participantIdentities:
                playerAccountId = player['player']['accountId']
                if playerAccountId not in summoner_dict:
                    continue
                summoner = summoner_dict[playerAccountId]
                dBSummoner = session.query(Summoner)\
                    .filter_by(accountId=playerAccountId)\
                    .first()
                if dBSummoner:
                    dBSummoner.rank.append(summoner['rank'])
                    dBSummoner.wins = summoner['wins']
                    dBSummoner.losses = summoner['losses']
                else:
                    new_summoners.append(Summoner(
                        rank=[summoner['rank']],
                        wins=summoner['wins'],
                        losses=summoner['losses'],
                        puuid=summoner['puuid'],
                        accountId=playerAccountId,
                    ))
                del summoner_dict[playerAccountId]
        print("Adding %s new summoner" % len(new_summoners))
        session.bulk_save_objects(new_summoners)
        session.commit()

    def get_matches(self, patch):
        session = self.engines[patch]['session']
        return session.query(Match.matchId).all()

    def add_matches(self, patch, matches, active_threads):
        session = self.engines[patch]['sessionmaker']()
        matchObjects = []
        teamObjects = []
        playerObjects = []
        runeObjects = []
        for match in matches:
            matchObjects.append(Match.create(match))
            teamObjects.append(Team.create(match, side=0))
            teamObjects.append(Team.create(match, side=1))
            elements = self.add_player_data(match)
            playerObjects += elements[0]
            runeObjects += elements[1]
        session.bulk_save_objects(matchObjects)
        session.bulk_save_objects(teamObjects)
        session.bulk_save_objects(playerObjects)
        session.bulk_save_objects(runeObjects)
        session.commit()
        active_threads[0] -= 1

    def add_player_data(self, match):
        matchId = match.matchId
        playerObjects = []
        runeObjects = []
        for i in range(1, 11):
            id = [pt for pt in match.participantIdentities if pt["participantId"] == i][0]['player']
            data = [pt for pt in match.participants if pt["participantId"] == i][0]
            playerObjects.append(Player.create(id, data, matchId, i))
            runeObjects += Runes.create(data, matchId, i)

        return playerObjects, runeObjects
