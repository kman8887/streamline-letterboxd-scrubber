import os
from pymongo import MongoClient

TMDB_KEY = os.getenv("TMDB_KEY")


def connect_to_database() -> tuple[str, MongoClient]:
    client: MongoClient = MongoClient(os.getenv("MONGODB_DB_URL"))
    db_name: str = os.getenv("MONGODB_DB_NAME")

    return db_name, client


def load_config():
    return f"host={os.getenv("HOST")} port=5432 password={os.getenv("POSTGRES_PASSWORD")} dbname={os.getenv("POSTGRES_DB")} user={os.getenv("POSTGRES_USER")}"
