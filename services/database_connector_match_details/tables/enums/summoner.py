"""Enums created for the Summoner Data Table."""
import enum


class Tier(enum.Enum):
    """Tier names mapped."""
    IRON = 0
    BRONZE = 1
    SILVER = 2
    GOLD = 3
    PLATINUM = 4
    DIAMOND = 5
    MASTER = 6
    GRANDMASTER = 7
    CHALLENGER = 8

    @classmethod
    def get(cls, tier):
        """Return enum value matching string provided."""
        return getattr(cls, tier)


class Rank(enum.Enum):
    """Rank names mapped."""
    IV = 0
    III = 1
    II = 2
    I = 3

    @classmethod
    def get(cls, rank):
        """Return enum value matching string provided."""
        return getattr(cls, rank)
