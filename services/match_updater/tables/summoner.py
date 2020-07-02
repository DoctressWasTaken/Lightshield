from sqlalchemy import Column, Integer, String, Enum
from . import Base
from .enums import Tier, Rank, Server

class Summoner(Base):
    __tablename__ = 'summoner'

    summonerId = Column(String(50))
    accountId = Column(String(56))
    puuid = Column(String(78), primary_key=True)

    summonerName = Column(String)
    tier = Column(Enum(Tier))
    rank = Column(Enum(Rank))

    series = Column(String(4))

    wins = Column(Integer)
    losses = Column(Integer)

    server = Column(Enum(Server))
