"""Tables related to Summoner Data."""
from sqlalchemy import Column, Integer, String, Enum
from . import Base
from .enums import Tier, Rank, Server


class Summoner(Base):  # pylint: disable=R0903
    """Summoner Details Merged from Summoner-V4 and League-V4."""
    __tablename__ = 'summoner'

    puuid = Column(String(78), primary_key=True)

    tier = Column(Enum(Tier))
    rank = Column(Enum(Rank))
    leaguePoints = Column(Integer)

    wins = Column(Integer)
    losses = Column(Integer)

    server = Column(Enum(Server))
