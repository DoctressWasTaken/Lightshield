"""Tables related to Summoner Data."""
from sqlalchemy import Column, Integer, String, Enum
from . import Base
from .enums import Tier, Rank, Server


class Summoner(Base):  # pylint: disable=R0903
    """Summoner Details Merged from Summoner-V4 and League-V4."""
    __tablename__ = 'summoner'

    tier = Column(Enum(Tier))
    rank = Column(Enum(Rank))
    leaguePoints = Column(Integer)

    wins = Column(Integer)
    losses = Column(Integer)
    summonerName = Column(String)
    puuid = Column(String(78))
    accountId = Column(String(56), primary_key=True)
    server = Column(Enum(Server), primary_key=True)
