import asyncio
from datetime import datetime

import aioredis


async def main():
    redis = aioredis.from_url(
        "redis://localhost:6379", encoding="utf-8", decode_responses=True
    )

    permit = await redis.get("lightshield_permit_handler")
    limits_init = await redis.get("lightshield_limits_init")
    limits_drop = await redis.get("lightshield_limits_drop")
    limits_update = await redis.get("lightshield_limits_update")
    update = await redis.get("lightshield_update_single")
    key_server = "server"
    key_zone = "zone"
    await redis.hsetnx(key_server, 'placeholder', 'H')
    await redis.hsetnx(key_zone, 'placeholder', 'H')

    timestamp = int(datetime.now().timestamp() * 1000)

    limit = [600, 100]

    # Test init
    await redis.evalsha(
        limits_init,
        1,
        key_zone,
        timestamp,
        *limit
    )
    # Test recreation
    await redis.evalsha(
        limits_init,
        1,
        key_zone,
        timestamp,
        *limit
    )
    # Test permit
    await redis.evalsha(
        permit,
        1,
        "zone",
        timestamp
    )
    # Test update
    count = 54
    await redis.evalsha(
        update,
        2,
        "zone:600",
        "zone:600:meta",
        timestamp,
        count
    )

    # Test update
    updated_limit = [600, 1000]
    await redis.evalsha(
        limits_update,
        1,
        key_zone,
        *updated_limit
    )
    # Test drop
    await redis.evalsha(
        limits_drop,
        1,
        key_zone,
        *[600, ]
    )


asyncio.run(main())
