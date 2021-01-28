"""Tables related to Match Data."""
from sqlalchemy import Column, Enum, Integer, Boolean, String, BigInteger, TIMESTAMP, SmallInteger, VARCHAR
from . import Base, Team
from .enums import Server
from sqlalchemy.orm import relationship


class Match(Base):
    """Match Base Element.

    Aims to resemble the LongtermStructure format.
    """
    __tablename__ = 'match'

    matchId = Column(BigInteger, primary_key=True)

    start = Column(BigInteger)
    duration = Column(SmallInteger)
    gameVersion = Column(String)

    win = Column(Boolean)  # False: Blue | True: Red

    @classmethod
    def create(cls, match):
        """Create the match object as well as sub elements"""
        matchObject = cls(
            start=match['gameCreation'] // 1000,
            duration=match['gameDuration'],
            gameVersion=match['gameVersion'],
            matchId=match['gameId'],
        )
        if match['teams'][0]['win'] == 'Win':
            matchObject.win = False
        else:
            matchObject.win = True
        return matchObject
