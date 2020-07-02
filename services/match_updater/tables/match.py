from sqlalchemy import Column, Integer, String, Enum, JSON, BigInteger
from . import Base
from .enums import Server

class Match(Base):

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

