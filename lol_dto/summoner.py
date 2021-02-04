from sqlalchemy import Column, String, ARRAY, SmallInteger, VARCHAR

from .base import Base


class Summoner(Base):
    """Summoner data table.

    Stores data on the users current ranking as well as history.
    Stores win/loss stats.
    Names are to be removed.

    puuid to identify the player.
    """

    __tablename__ = 'summoner'

    puuid = Column(String(78))
    account_id = Column(String(56))
    summoner_id = Column(String(63), primary_key=True)

    rank = Column("rank", SmallInteger)  # Calculated summed LP from lowest rank
    rank_history = Column('rank_history', ARRAY(SmallInteger))

    wins = Column("wins", SmallInteger)
    losses = Column("losses", SmallInteger)

    wins_p = Column("wins_diff", SmallInteger)
    losses_diff = Column("losses_diff", SmallInteger)

    priority = Column("priority", VARCHAR(1))
