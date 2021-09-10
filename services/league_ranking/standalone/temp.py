import asyncpg
import asyncio




async def main():
    await asyncpg.create_pool(
        host='85.214.161.229',
        port=6379
    )




asyncio.run(main())
