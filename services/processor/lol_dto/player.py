from . import Base
from .enums import Server
from sqlalchemy import Column, Enum, Boolean, BigInteger, SmallInteger, ForeignKey, VARCHAR, Integer, ARRAY
from sqlalchemy.orm import relationship


class Player(Base):
    """Basic player data class."""

    __tablename__ = 'player'

    matchId = Column(BigInteger, ForeignKey('match.matchId'), primary_key=True)
    participantId = Column(SmallInteger, primary_key=True)

    accountId = Column(VARCHAR(56), index=True)
    championId = Column(SmallInteger, index=True)
    team = Column(Boolean)  # False: Blue | True: Red
    statPerks = Column(ARRAY(SmallInteger))
    summonerSpells = Column(ARRAY(SmallInteger))
    items = Column(ARRAY(SmallInteger))

    # Stats
    firstBlood = Column(Boolean)
    firstBloodAssist = Column(Boolean)
    kills = Column(SmallInteger)
    deaths = Column(SmallInteger)
    assists = Column(SmallInteger)
    gold = Column(Integer)
    cs = Column(SmallInteger)
    level = Column(SmallInteger)
    wardsPlaced = Column(SmallInteger)
    wardsKilled = Column(SmallInteger)
    visionWardsBought = Column(SmallInteger)
    visionScore = Column(SmallInteger)
    killingSprees = Column(SmallInteger)
    largestKillingSpree = Column(SmallInteger)
    doubleKills = Column(SmallInteger)
    tripleKills = Column(SmallInteger)
    quadraKills = Column(SmallInteger)
    pentaKills = Column(SmallInteger)
    monsterKills = Column(SmallInteger)
    monsterKillsInAlliedJungle = Column(SmallInteger)
    monsterKillsInEnemyJungle = Column(SmallInteger)
    totalDamageDealt = Column(Integer)
    physicalDamageDealt = Column(Integer)
    magicDamageDealt = Column(Integer)
    totalDamageDealtToChampions = Column(Integer)
    physicalDamageDealtToChampions = Column(Integer)
    magicDamageDealtToChampions = Column(Integer)
    damageDealtToObjectives = Column(Integer)
    damageDealtToTurrets = Column(Integer)
    totalDamageTaken = Column(Integer)
    physicalDamageTaken = Column(Integer)
    magicDamageTaken = Column(Integer)
    longestTimeSpentLiving = Column(SmallInteger)
    largestCriticalStrike = Column(SmallInteger)
    goldSpent = Column(Integer)
    totalHeal = Column(Integer)
    totalUnitsHealed = Column(SmallInteger)
    damageSelfMitigated = Column(Integer)
    totalTimeCCDealt = Column(SmallInteger)
    timeCCingOthers = Column(SmallInteger)
    firstTower = Column(Boolean)
    firstTowerAssist = Column(Boolean)
    firstInhibitor = Column(Boolean)
    firstInhibitorAssist = Column(Boolean)

    # Timeline
    csDiffPerMinDeltas = Column(ARRAY(SmallInteger))
    damageTakenPerMinDeltas = Column(ARRAY(SmallInteger))
    damageTakenDiffPerMinDeltas = Column(ARRAY(SmallInteger))
    xpPerMinDeltas = Column(ARRAY(SmallInteger))
    xpDiffPerMinDeltas = Column(ARRAY(SmallInteger))
    creepsPerMinDeltas = Column(ARRAY(SmallInteger))
    goldPerMinDeltas = Column(ARRAY(SmallInteger))

    @classmethod
    def create(cls, id, data, matchId, participantId):

        playerObject = cls(
            participantId=participantId,
            matchId=matchId,
            accountId=id['currentAccountId'],
            championId=data['championId'],
            team=data['teamId'] == 200,
            statPerks=[
                data['stats']['statPerk0'],
                data['stats']['statPerk1'],
                data['stats']['statPerk2']],
            summonerSpells=[data['spell1Id'], data['spell2Id']],
            items=[data['stats']['item%s' % i] for i in range(7)]
        )

        for key in data['stats']:
            if hasattr(Player, key):
                setattr(playerObject, key, data['stats'][key])

        for entry in data['timeline']:
            if entry.endswith('Deltas'):
                setattr(playerObject, entry, data['timeline'][entry].values())
        return playerObject


class Runes(Base):
    """Runes used by the player."""

    __tablename__ = 'runes'

    matchId = Column(BigInteger, ForeignKey('match.matchId'), primary_key=True)
    participantId = Column(SmallInteger, primary_key=True)
    position = Column(VARCHAR(1), primary_key=True)
    runeId = Column(SmallInteger)
    stats1 = Column(Integer)
    stats2 = Column(Integer)
    stats3 = Column(Integer)

    @classmethod
    def create(cls, data, matchId, participantId):
        runeObjects = []
        for i in range(6):
            runeObjects.append(cls(
                matchId=matchId,
                participantId=participantId,
                position=i,
                runeId=data['stats']['perk%s' % i],
                stats1=data['stats']['perk%sVar1' % i],
                stats2=data['stats']['perk%sVar2' % i],
                stats3=data['stats']['perk%sVar3' % i],
            ))
        return runeObjects
