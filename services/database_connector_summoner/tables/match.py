"""Tables related to Match Data."""
from sqlalchemy import Column, Integer, String, Enum, BigInteger, JSON, DateTime
from . import Base
from .enums import Server
import datetime


class Match(Base):  # pylint: disable=R0903
    """Match-V4: Match-Details Table."""
    __tablename__ = 'match'

    matchId = Column(BigInteger, primary_key=True)
    queue = Column(Integer)
    gameDuration = Column(Integer)
    server = Column(Enum(Server), primary_key=True)
    gameCreation = Column(BigInteger)
    seasonId = Column(Integer)
    gameVersion = Column(String(20))
    mapId = Column(Integer)
    gameMode = Column(String(15))

    participantIdentities = Column(JSON)
    teams = Column(JSON)
    participants = Column(JSON)

    matchAdded = Column(DateTime, default=datetime.datetime.utcnow, index=True)
