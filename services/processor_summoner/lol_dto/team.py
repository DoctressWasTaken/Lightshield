from . import Base, Player
from sqlalchemy import Column, String, Boolean, BigInteger, SmallInteger, ForeignKey, VARCHAR, Integer, JSON
from sqlalchemy.orm import relationship


class Team(Base):
    """Team wide data and player in said team."""

    __tablename__ = 'team'

    matchId = Column(BigInteger, ForeignKey('match.matchId'), primary_key=True)
    side = Column(Boolean, primary_key=True)  # False: Blue | True: Red
    bans = Column(JSON)

    riftHeraldKills = Column(VARCHAR(1))
    dragonKills = Column(SmallInteger)
    baronKills = Column(SmallInteger)
    towerKills = Column(SmallInteger)
    inhibitorKills = Column(SmallInteger)
    firstTower = Column(Boolean)
    firstInhibitor = Column(Boolean)
    firstRiftHerald = Column(Boolean)
    firstDragon = Column(Boolean)
    firstBaron = Column(Boolean)

    @classmethod
    async def create(cls, match, side):
        teamData = match['teams'][side]
        teamObject = cls(
            matchId=match['gameId'],
            side=side,
            bans=teamData['bans']
        )

        for key in teamData:
            if hasattr(teamObject, key):
                setattr(teamObject, key, teamData[key])

        return teamObject
