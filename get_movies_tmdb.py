import datetime
import asyncio
import os
from typing import Dict, List
from aiohttp import ClientSession
from pprint import pprint
from tqdm import tqdm
from pymongo import UpdateOne, database
from pymongo.synchronous.collection import Collection
from pymongo.errors import BulkWriteError
import math
from database import connect_to_database


async def fetch_json(url, session, headers, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception("Url returned not ok")
        except Exception as e:
            attempt += 1
            print(f"\nAttempt {attempt} failed for URL {url} with exception: {repr(e)}\n")
            if attempt == retries:
                print(f"\nMax retries reached for URL {url}\n")
                return None
            await asyncio.sleep(2)


async def get_num_of_pages_and_results(url: str, headers: Dict[str, str]) -> tuple[int, int]:
    async with ClientSession() as session:
        first_result = await fetch_json(url, session, headers)

    return first_result["total_pages"], first_result["total_results"]


async def get_db_operations(results: List[Dict]) -> List[UpdateOne]:
    results = results["results"]
    db_operations = []

    for result in results:

        movie_object = {"tmdb_id": result["id"]}

        simple_fields = [
            "genre_ids",
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
        ]

        for field_name in simple_fields:
            try:
                movie_object[field_name] = result[field_name]
            except:
                movie_object[field_name] = None

        movie_object["last_updated"] = datetime.datetime.now()

        db_operations.append(
            UpdateOne({"tmdb_id": result["id"]}, {"$set": movie_object}, upsert=True)
        )

    return db_operations


async def fetch_and_store_movies(
    urls: List[str],
    headers: Dict[str, str],
    db: database.Database,
    batch_size: int = 20,
    retries: int = 3,
):
    total_batches = math.ceil(len(urls) / batch_size)
    pbar = tqdm(range(total_batches), position=0)

    for batch_index in pbar:
        pbar.set_description(f"Gathering movie data for batch {batch_index+1} of {total_batches}")
        batch_urls = urls[batch_index * batch_size : (batch_index + 1) * batch_size]
        results = [None] * len(batch_urls)
        failed_urls = batch_urls.copy()

        for retry in range(retries):
            try:
                async with ClientSession() as session:
                    if len(failed_urls) < len(batch_urls):
                        print(f"\nRetrying {len(failed_urls)} urls\n")
                    tasks = [
                        asyncio.ensure_future(fetch_json(url, session, headers))
                        for url in failed_urls
                    ]
                    batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Update results and track failed URLs
                    failed_urls = []
                    for i, result in enumerate(batch_results):
                        if isinstance(result, Exception) or result is tuple[None, None]:
                            failed_urls.append(urls[i])  # Retry this URL
                        else:
                            results[i] = result

                    # Exit early if all URLs succeeded
                    if len(failed_urls) <= 0:
                        break

            except Exception as session_error:
                print(f"Session failed with exception: {repr(session_error)}")
                print("Attempting to refresh session...")
                await asyncio.sleep(2)  # Wait before retrying with a new session

        if len(failed_urls) > 0:
            print(f"\nThere were {len(failed_urls)} failed urls\n")

        tasks = []
        for result in results:
            task = asyncio.ensure_future(get_db_operations(result))
            tasks.append(task)

        # Gather all ratings page responses
        upsert_operations = await asyncio.gather(*tasks)

        flattened_operations = [op for sublist in upsert_operations for op in sublist]

        try:
            if len(flattened_operations) > 0:
                # Create/reference "ratings" collection in db
                movies: Collection = db["movies"]
                movies.bulk_write(flattened_operations, ordered=False)
        except BulkWriteError as bwe:
            pprint(bwe.details)
        await asyncio.sleep(1)


async def fetch_tmdb_data(
    db: database.Database,
    release_date_to: datetime.date | None,
    release_date_from: datetime.date,
    retries=3,
) -> tuple[datetime.date | None, datetime.date | None]:
    if release_date_to:
        base_url = (
            "https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&page={}&primary_release_date.gte="
            + release_date_from.isoformat()
            + "&primary_release_date.lte="
            + release_date_to.isoformat()
            + "&sort_by=primary_release_date.desc&vote_count.gte=15"
        )
    else:
        base_url = (
            "https://api.themoviedb.org/3/discover/movie?include_adult=false&include_video=false&language=en-US&page={}&primary_release_date.gte="
            + release_date_from.isoformat()
            + "&sort_by=primary_release_date.desc&vote_count.gte=15"
        )

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {os.getenv('TMDB_TOKEN')}",
    }

    num_of_pages, num_of_results = await get_num_of_pages_and_results(base_url.format(1), headers)

    print(f"number of pages {num_of_pages}")
    print(f"num of results: {num_of_results}")

    if num_of_pages > 500:
        # if there is more than 500 pages then we shorten the range to 1 year
        return release_date_to, release_date_from.replace(release_date_from.year + 1)
    elif num_of_results == 0:
        print(
            f"0 results found for the range: {release_date_from.isoformat()}, {release_date_to.isoformat()}"
        )
        return None, None

    urls = []

    for i in range(num_of_pages):
        urls.append(base_url.format(i + 1))

    await fetch_and_store_movies(urls, headers, db)

    return release_date_from, release_date_from.replace(release_date_from.year - 2)


async def get_initial_movie_dataset():
    db_name, client = connect_to_database()
    db = client[db_name]

    movies = db["movies"]

    last_movie_in_db = None

    last_movie_in_db = list(movies.find({}).sort({"release_date": 1}).limit(1))[0]

    release_date_to: datetime.date
    release_date_from: datetime.date

    release_date_to = None

    if last_movie_in_db:
        release_date = last_movie_in_db["release_date"]
        datetime_object: datetime.date = datetime.datetime.strptime(release_date, "%Y-%m-%d").date()
        release_date_to = datetime.date((datetime_object.year + 1), 1, 1)
        release_date_from = release_date_to.replace(release_date_to.year - 2)
    else:
        release_date_from = datetime.date((datetime.date.today().year - 2), 1, 1)

    print(release_date_from)

    while True:
        release_date_to, release_date_from = await fetch_tmdb_data(
            db, release_date_to, release_date_from
        )
        if not release_date_to:
            break


def main():
    asyncio.run(get_initial_movie_dataset())


if __name__ == "__main__":
    main()
