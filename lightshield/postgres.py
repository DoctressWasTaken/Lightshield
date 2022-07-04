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
        "\nAll content in the database `%s` will be overwritten, are you sure? [yes/no] " % psq_con.database
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

    logger.info("Generating schemas")

    query_files = {}
    per_platform = os.path.join(
        os.path.dirname(__file__), "postgres_templates", "per_platform"
    )
    for file in os.listdir(per_platform):
        with open(os.path.join(per_platform, file)) as content:
            query_files[file] = content.read()
    schema_query = """
                DROP SCHEMA IF EXISTS %s CASCADE;
                CREATE SCHEMA IF NOT EXISTS %s;
                GRANT ALL PRIVILEGES ON SCHEMA %s TO %s;
            """
    async with db.acquire() as connection:
        for platform in enums.platforms:
            await connection.execute(
                schema_query
                % (
                    platform,
                    platform,
                    platform,
                    psq_con.user,
                )
            )
            logger.info("Generated schema %s", platform)
            for name, query in query_files.items():
                await connection.execute(query.replace("PLATFORM", platform))
                logger.info("\t- Table %s", name)
    query_files = {}
    per_region = os.path.join(
        os.path.dirname(__file__), "postgres_templates", "per_region"
    )
    for file in os.listdir(per_region):
        with open(os.path.join(per_region, file)) as content:
            query_files[file] = content.read()
    async with db.acquire() as connection:
        for region in enums.regions:
            await connection.execute(
                schema_query
                % (
                    region,
                    region,
                    region,
                    psq_con.user,
                )
            )
            logger.info("Generated schema %s", region)
            for name, query in query_files.items():
                await connection.execute(query.replace("REGION", region))
                logger.info("\t- Table %s", name)
    async with db.acquire() as connection:
        central = os.path.join(
            os.path.dirname(__file__), "postgres_templates", "central"
        )
        for file in os.listdir(central):
            with open(os.path.join(central, file)) as content:
                await connection.execute(content.read())
