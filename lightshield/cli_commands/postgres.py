import json
import logging
import os
from pprint import PrettyPrinter

import asyncpg
from asyncpg.exceptions import DuplicateDatabaseError

pp = PrettyPrinter(indent=2)

logger = logging.getLogger("Postgres")


async def init_db(config, **kwargs):
    psq_con = config.connections.postgres
    logger.info("Found the following postgres connection details.")
    print(json.dumps(psq_con.__dict__, indent=4))
    if input("\nAre those details correct? [yes/no] ").lower() not in ["y", "yes"]:
        logger.info("Exiting...")
        exit()

    if input(
        "\nDoes the %s database already exist? "
        "If not it will be created which requires elevated user rights. [yes/no] "
        % psq_con.database
    ).lower() not in ["y", "yes"]:
        logger.info("Attempting to generate the database")
        db_creator = await asyncpg.create_pool(
            host=psq_con.hostname,
            port=psq_con.port,
            user=psq_con.user,
            password=os.getenv(psq_con.password_env),
        )
        async with db_creator.acquire() as connection:
            try:
                await connection.execute("CREATE DATABASE %s" % psq_con.database)
                await connection.execute(
                    "GRANT ALL PRIVILEGES ON DATABASE %s TO %s"
                    % (psq_con.database, psq_con.user)
                )
            except DuplicateDatabaseError as err:
                print(
                    "\nDatabase already exists. Do you want to continue anyway. This will overwrite all data."
                )
        await db_creator.close()
    if input(
        "\nAll content in the database `%s` will be overwritten, are you sure? [yes/no] "
        % psq_con.database
    ).lower() not in ["y", "yes"]:
        exit()
    db = await asyncpg.create_pool(
        host=psq_con.hostname,
        port=psq_con.port,
        user=psq_con.user,
        database=psq_con.database,
        password=os.getenv(psq_con.password_env),
    )
    # Generate the database
    logger.info("Generating enums")
    enums = config.statics.enums
    for enum, values in enums.__dict__.items():
        async with db.acquire() as connection:
            query = "DROP TYPE IF EXISTS %s CASCADE; CREATE TYPE %s AS ENUM %s" % (
                enum,
                enum,
                tuple(values),
            )
            await connection.execute(query)

    path = os.path.join(os.path.dirname(__file__), "../postgres_templates")
    files = os.listdir(path)

    async with db.acquire() as connection:
        for file in files:
            if not file.endswith("_partition.sql"):
                logger.info("Generated %s", file)
                with open(os.path.join(path, file)) as sql_file:
                    sql = sql_file.read()
                    logger.debug(sql)
                    await connection.execute(sql)
                if "%s_partition.sql" % file.strip(".sql") in files:
                    with open(
                        os.path.join(path, "%s_partition.sql" % file.strip(".sql")),
                        encoding="utf-8",
                    ) as partition_sql:
                        sql_string = partition_sql.read()
                        for platform in config.statics.enums.platforms:
                            sql = sql_string.format(
                                platform=platform.lower(), platform_caps=platform
                            )
                            logger.debug(sql)
                            await connection.execute(sql)
                            logger.info("\t> %s", platform)
