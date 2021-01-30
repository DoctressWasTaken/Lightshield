from lol_dto import (
    Base, Summoner, Match, Team, Player, Runes)
from sqlalchemy.ext.asyncio import create_async_engine


class PermanentDB:
    base_url = "postgresql+asyncpg://postgres@postgres/raw"

    def __init__(self):
        self.engine = None


    async def init(self):
        self.engine = create_async_engine(
            self.base_url, echo=True,
        )

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

