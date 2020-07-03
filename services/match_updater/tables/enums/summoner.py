import enum

class Tier(enum.Enum):
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
        return getattr(cls, tier)

class Rank(enum.Enum):
    IV = 0
    III = 1
    II = 2
    I = 3

    @classmethod
    def get(cls, rank):
        return getattr(cls, rank)


