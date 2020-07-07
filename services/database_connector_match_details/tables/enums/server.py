"""Enum containing Server values mapped."""
import enum


class Server(enum.Enum):
    """Server codes mapped."""
    BR1 = 0
    EUN1 = 1
    EUW1 = 2
    JP1 = 3
    KR = 4
    LA1 = 5
    LA2 = 6
    NA1 = 7
    OC1 = 8
    TR1 = 9
    RU = 10

    @classmethod
    def get(cls, server):
        """Return Enum value matching string provided."""
        return getattr(cls, server)
