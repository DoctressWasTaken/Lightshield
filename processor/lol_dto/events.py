from . import Base


class BaseEvent(Base):
    """Basic abstract class inherited to event classes."""


class ItemEvents(BaseEvent):
    """Events marked as Item events."""


class WardsEvents(BaseEvent):
    """Events marked as Ward events."""


class SkillsEvents(BaseEvent):
    """Events marked as Skill events."""


class KillEvents(BaseEvent):
    """Events related to kills & executes."""
