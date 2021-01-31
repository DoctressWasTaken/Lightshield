from . import Base
from .enums import Server
from sqlalchemy import Column, Enum, Boolean, BigInteger, SmallInteger, ForeignKey, VARCHAR, Integer, ARRAY
from sqlalchemy.orm import relationship


class Player(Base):
    """Basic player data class."""

    __tablename__ = 'player'

    matchId = Column(BigInteger, primary_key=True)
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
    async def create(cls, match, participantId):
        participant = match['participants'][participantId - 1]
        partId = match['participantIdentities'][participantId - 1]['player']

        playerObject = cls(
            participantId=participantId,
            matchId=match['gameId'],
            accountId=partId['currentAccountId'],
            championId=participant['championId'],
            team=participantId > 5,
            statPerks=[
                participant['stats']['statPerk0'],
                participant['stats']['statPerk1'],
                participant['stats']['statPerk2']],
            summonerSpells=[participant['spell1Id'], participant['spell2Id']],
            items=[participant['stats']['item%s' % i] for i in range(7)]
        )

        for key in participant['stats']:
            if hasattr(Player, key):
                setattr(playerObject, key, participant['stats'][key])

        for entry in participant['timeline']:
            if entry.endswith('Deltas'):
                setattr(playerObject, entry, participant['timeline'][entry].values())
        return playerObject


class Runes(Base):
    """Runes used by the player."""

    __tablename__ = 'runes'

    matchId = Column(BigInteger, primary_key=True)
    participantId = Column(SmallInteger, primary_key=True)
    position = Column(SmallInteger, primary_key=True)
    runeId = Column(SmallInteger)
    stats1 = Column(Integer)
    stats2 = Column(Integer)
    stats3 = Column(Integer)

    @classmethod
    async def create(cls, match, participantId):
        participant = match['participants'][participantId - 1]

        runeObjects = []
        for i in range(6):
            runeObjects.append(cls(
                matchId=match['gameId'],
                participantId=participantId,
                position=i,
                runeId=participant['stats']['perk%s' % i],
                stats1=participant['stats']['perk%sVar1' % i],
                stats2=participant['stats']['perk%sVar2' % i],
                stats3=participant['stats']['perk%sVar3' % i],
            ))
        return runeObjects
