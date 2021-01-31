from sqlalchemy.ext.asyncio import create_async_engine

from lol_dto import (
    Base)


class PermanentDB:
    base_url = "postgresql+asyncpg://postgres@postgres/raw"

    def __init__(self):
        self.engine = None

    async def init(self):
        self.engine = create_async_engine(
            self.base_url, echo=False,
        )

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
