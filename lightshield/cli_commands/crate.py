import json
import logging
import os

import asyncpg

logger = logging.getLogger("CrateDB")


async def init_db(config, **kwargs):
    psq_con = config.db
    logger.info("Found the following crate connection details.")
    print(json.dumps(psq_con.__dict__, indent=4))
    if input("\nAre those details correct? [yes/no] ").lower() not in ["y", "yes"]:
        logger.info("Exiting...")
        exit()

    if input(
        "\nAll content in the `%s` tables will be overwritten, are you sure? [yes/no] "
        % psq_con.schema
    ).lower() not in ["y", "yes"]:
        exit()
    db = await asyncpg.create_pool(
        host=psq_con.host,
        port=psq_con.port,
        user=psq_con.user,
        password=psq_con.password,
    )
    # Generate the tables
    path = os.path.join(os.path.dirname(__file__), "../crate_templates")
    files = os.listdir(path)

    async with db.acquire() as connection:
        for file in files:
            logger.info("Generated %s", file)
            with open(os.path.join(path, file)) as sql_file:
                sql = sql_file.read()
                sql = sql.replace("{{schema}}", psq_con.schema)

                await connection.execute(sql)
