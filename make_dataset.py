import os
from bs4 import BeautifulSoup
from aiohttp import ClientSession
import asyncio
from database import connect_to_database
from tqdm import tqdm
import datetime
from pymongo import UpdateOne, UpdateMany
from pymongo.errors import BulkWriteError
from tmdb_connector import fetch_and_store, fetch_json

CHUNK_SIZE = 40
TMDB_KEY = os.getenv("TMDB_KEY")


def main():
    result = asyncio.run(main())
    soup = BeautifulSoup(result, "lxml")
    with open("output_film.html", "w") as file:
        file.write(str(soup))


# Data sanitisation, deleting users that have reviewed almost every movie
def delete_new_archive_query():
    users_to_delete = ["solidaritycine", "newarchive", "whatsthebudget", "fantic"]
    db_name, client = connect_to_database()

    db = client[db_name]
    users = db["letterboxd_users"]
    movies = db["letterboxd_movies"]
    interactions = db["letterboxd_interactions"]

    for user in users_to_delete:
        users.delete_one({"username": user})
        interactions.delete_many({"letterboxd_username": user})


def delete_only_one_interaction_movies():
    db_name, client = connect_to_database()
    db = client[db_name]

    db.letterboxd_movies.delete_many(
        {"letterboxd_movie_id": {"$in": db.temp_movies_to_delete_2.distinct("letterboxd_movie_id")}}
    )


async def fetch_json_value(url, session: ClientSession, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    if response.status == 404:
                        response_json = await response.json()
                        if response_json and response_json["status_code"] == 34:
                            return None
                        else:
                            print(response_json)
                            print("Resource not found")
                    else:
                        raise Exception("Url returned not ok")
        except Exception as e:
            attempt += 1
            print(f"\nAttempt {attempt} failed for URL {url} with exception: {repr(e)}\n")
            if attempt == retries:
                print(f"\nMax retries reached for URL {url}\n")
                return None
            await asyncio.sleep(2)


async def fetch_tmdb_details(url, session: ClientSession):
    response = await fetch_json_value(url, session)

    if response == None:
        return None

    movie_object = {}

    renamed_fields = [{"response_name": "genres", "db_field": "genre_ids"}]
    simple_fields = [
        "backdrop_path",
        "poster_path",
        "popularity",
        "overview",
        "vote_average",
        "vote_count",
        "release_date",
        "original_language",
        "original_title",
        "title",
        "adult",
        "video",
    ]

    for field_name in simple_fields:
        try:
            movie_object[field_name] = response[field_name]
        except:
            movie_object[field_name] = None

    for field_object in renamed_fields:
        try:
            items = [x["id"] for x in response[field_object["response_name"]]]
            movie_object[field_object["db_field"]] = items
        except:
            movie_object[field_object["db_field"]] = None

    movie_object["last_updated"] = datetime.datetime.now()

    return UpdateOne({"tmdb_id": str(response["id"])}, {"$set": movie_object})


async def fetch_and_store_movies(found_movies, tmdb_movies):
    url_template = "https://api.themoviedb.org/3/movie/{}?api_key={}"

    async with ClientSession() as session:
        for i in tqdm(range(0, len(found_movies), CHUNK_SIZE)):
            chunk = found_movies[i : i + CHUNK_SIZE]
            tasks = [
                fetch_tmdb_details(url_template.format(movie["tmdb_id"], TMDB_KEY), session)
                for movie in chunk
            ]

            results = await asyncio.gather(*tasks)

            # Filter out failed requests (None values)
            updates = [r for r in results if r]

            if updates:
                try:
                    tmdb_movies.bulk_write(updates, ordered=False)
                except BulkWriteError as bwe:
                    print("MongoDB Error:", bwe.details)

            await asyncio.sleep(1)


async def clean_up_movies():
    db_name, client = connect_to_database()
    db = client[db_name]

    tmdb_movies = db.temp_movies_not_in_tmdb
    found_movies = [x for x in list(tmdb_movies.find({"interaction_count": {"$gte": 10}}))]

    await fetch_and_store_movies(found_movies, tmdb_movies)


async def fetch_watch_providers(url, session: ClientSession):
    response = await fetch_json_value(url, session)

    if response == None:
        return None

    watch_providers = {}

    watch_providers["watch_providers"] = response["results"]
    watch_providers["last_updated"] = datetime.datetime.now()

    return UpdateOne({"tmdb_id": str(response["id"])}, {"$set": watch_providers})


async def fetch_and_store_watch_providers(found_movies, tmdb_movies):
    url_template = "https://api.themoviedb.org/3/movie/{}/watch/providers?api_key={}"
    # found_movies = [{"tmdb_id": "1000548"}]
    async with ClientSession() as session:
        for i in tqdm(range(0, len(found_movies), CHUNK_SIZE)):
            chunk = found_movies[i : i + CHUNK_SIZE]
            tasks = [
                fetch_watch_providers(url_template.format(movie["tmdb_id"], TMDB_KEY), session)
                for movie in chunk
            ]

            results = await asyncio.gather(*tasks)

            # Filter out failed requests (None values)
            updates = [r for r in results if r]
            if updates:
                try:
                    tmdb_movies.bulk_write(updates, ordered=False)
                except BulkWriteError as bwe:
                    print("MongoDB Error:", bwe.details)


async def add_watch_providers():
    db_name, client = connect_to_database()
    db = client[db_name]

    tmdb_movies = db.temp_movies_not_in_tmdb
    filter = {
        "$and": [
            {"watch_providers": {"$exists": False}},
            {"video": {"$exists": True}},
            {"video": False},
            {"adult": False},
        ]
    }
    sort = list({"popularity": -1}.items())

    found_movies = [x for x in list(tmdb_movies.find(filter=filter, sort=sort))]

    await fetch_and_store_watch_providers(found_movies, tmdb_movies)


async def make_tmdb_id_int():
    db_name, client = connect_to_database()
    db = client[db_name]
    collection = db["letterboxd_movies"]

    collection.update_many(
        {
            "tmdb_id": {"$type": "string"},
            "tmdb_id": {"$ne": ""},
        },  # Filter only string tmdb_id
        [{"$set": {"tmdb_id": {"$toInt": "$tmdb_id"}}}],  # Convert to integer
    )


async def move_to_movies_table():
    db_name, client = connect_to_database()
    filter_conditions = {
        "$and": [
            {"watch_providers": {"$exists": True}},
            {"watch_providers": {"$ne": {}}},
            {"vote_count": {"$ne": 0}},
        ]
    }

    project_fields = {"_id": 0}  # Adjust projection as needed

    sort_order = {"popularity": 1, "vote_count": -1}

    client[db_name]["temp_movies_not_in_tmdb"].aggregate(
        [
            {"$match": filter_conditions},
            {"$sort": sort_order},
            {"$project": project_fields},
            {
                "$merge": {
                    "into": "movies",  # Target collection
                    "on": "tmdb_id",
                    "whenMatched": "merge",  # Update existing docs
                    "whenNotMatched": "insert",  # Insert new docs
                }
            },
        ]
    )


async def delete_duplicates():
    db_name, client = connect_to_database()

    client[db_name]["movies"].delete_many({"adult": {"$exists": True}})


async def merge_letterboxd_movies_table():
    db_name, client = connect_to_database()

    project_fields = {"_id": 0, "imdb_link": 0, "movie_title": 0, "tmdb_link": 0}

    client[db_name]["letterboxd_movies"].aggregate(
        [
            {
                "$match": {"tmdb_id": {"$type": "int"}},
            },
            {"$project": project_fields},
            {
                "$merge": {
                    "into": "movies",  # Target collection
                    "on": "tmdb_id",
                    "whenMatched": "merge",  # Update existing docs
                    "whenNotMatched": "discard",  # Insert new docs
                }
            },
        ]
    )


async def fetch_movie_details(url: str, session: ClientSession):
    response = await fetch_json(url, session)

    if response == None:
        return None

    movie_details = dict(response)

    tmdb_id = movie_details.pop("id")
    watch_providers = movie_details.pop("watch/providers")["results"]
    movie_details["tmdb_id"] = tmdb_id
    movie_details["watch_providers"] = watch_providers
    movie_details["last_updated"] = datetime.datetime.now()

    return UpdateOne({"tmdb_id": response["id"]}, {"$set": movie_details})


async def add_movie_details():
    db_name, client = connect_to_database()
    db = client[db_name]

    tmdb_movies = db.movies
    found_movies = [x for x in list(tmdb_movies.find({"credits": {"$exists": False}}))]
    # found_movies = [{"tmdb_id": 591}]

    base_url = "https://api.themoviedb.org/3/movie/{}?append_to_response=external_ids%2Ckeywords%2Creviews%2Ccredits%2Cwatch%2Fproviders&language=en-GB&api_key={}"

    urls = [base_url.format(movie["tmdb_id"], TMDB_KEY) for movie in found_movies]

    await fetch_and_store(urls, tmdb_movies, 45, fetch_movie_details)


def unwind_keywords():
    db_name, client = connect_to_database()
    db = client[db_name]
    db.movies.update_many({}, [{"$set": {"keywords": "$keywords.keywords"}}])


if __name__ == "__main__":
    asyncio.run(unwind_keywords())
