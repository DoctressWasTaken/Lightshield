from .base import Base

from sqlalchemy import Column, String, Integer, ARRAY, SmallInteger, Enum
from sqlalchemy.orm import relationship
from .enums import Server


class Summoner(Base):
    """Summoner data table.

    Stores data on the users current ranking as well as history.
    Stores win/loss stats.
    Names are to be removed.

    puuid to identify the player.
    """

    __tablename__ = 'summoner'

    puuid = Column(String(78), primary_key=True)
    rank = Column("rank", ARRAY(SmallInteger))  # Calculated summed LP from lowest rank
    wins = Column("wins", SmallInteger)
    losses = Column("losses", SmallInteger)

    account_id = Column(String(56))
