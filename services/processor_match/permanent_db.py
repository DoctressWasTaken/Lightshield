from lol_dto import (
    Base, Summoner, Match, Team, Player, Runes)
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists


class PermanentDB:
    base_url = "postgresql://postgres@postgres/raw"

    def __init__(self):
        self.engine = None
        engine = create_engine(self.base_url)
        if not database_exists(engine.url):
            print("Created db")
            create_database(engine.url)
        Base.metadata.create_all(engine)
