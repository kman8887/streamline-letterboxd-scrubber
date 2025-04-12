#!/usr/local/bin/python3.11

from tqdm import tqdm
import math
import datetime
from itertools import chain

import asyncio
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from pprint import pprint

from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from database import connect_to_database

from enum import Enum


class Interactions(str, Enum):
    RATING = "RATING"
    LIKE = "LIKE"
    REVIEW = "REVIEW"
    WATCHED = "WATCHED"


async def fetch(url, session, input_data={}, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(url) as response:
                return await response.read(), input_data
        except Exception as e:
            attempt += 1
            print(f"\nAttempt {attempt} failed for URL {url} with exception: {repr(e)}\n")
            if attempt == retries:
                print(f"\nMax retries reached for URL {url}\n")
                return None, None
            await asyncio.sleep(2)


async def get_page_counts(usernames, users_cursor):
    url = "https://letterboxd.com/{}/films/"
    tasks = []

    async with ClientSession() as session:
        for username in usernames:
            task = asyncio.ensure_future(
                fetch(url.format(username), session, {"username": username})
            )
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        responses = [x for x in responses if x]

        update_operations = []
        for i, response in enumerate(responses):
            soup = BeautifulSoup(response[0], "lxml")
            try:
                page_link = soup.findAll("li", attrs={"class", "paginate-page"})[-1]
                num_pages = int(page_link.find("a").text.replace(",", ""))
            except IndexError:
                num_pages = 1

            user = users_cursor.find_one({"username": response[1]["username"]})

            try:
                previous_num_pages = user["num_ratings_pages"]
                if not previous_num_pages:
                    previous_num_pages = 0
            except KeyError:
                previous_num_pages = 0

            new_pages = min(num_pages, num_pages - previous_num_pages + 1)

            if num_pages >= 128 and new_pages < 10:
                new_pages = 10

            update_operations.append(
                UpdateOne(
                    {"username": response[1]["username"]},
                    {
                        "$set": {
                            "num_ratings_pages": num_pages,
                            "recent_page_count": new_pages,
                            "last_updated": datetime.datetime.now(),
                        }
                    },
                    upsert=True,
                )
            )

        try:
            if len(update_operations) > 0:
                users_cursor.bulk_write(update_operations, ordered=False)
        except BulkWriteError as bwe:
            pprint(bwe.details)


def get_user_rating_interaction(review, movie_id, user_id):
    rating = review.find("span", attrs={"class": "rating"})
    if not rating:
        return
    else:
        rating_class = rating["class"][-1]
        rating_val = int(rating_class.split("-")[-1])

    return {
        "letterboxd_movie_id": movie_id,
        "interaction_type": Interactions.RATING.value,
        "rating_val": rating_val,
        "letterboxd_username": user_id,
    }


def get_like_interaction(review, movie_id, user_id):
    like = review.find("span", attrs={"class": "like"})
    if not like:
        return
    else:
        return {
            "letterboxd_movie_id": movie_id,
            "interaction_type": Interactions.LIKE.value,
            "letterboxd_username": user_id,
        }


def get_reviewed_interaction(review, movie_id, user_id):
    review = review.find("a", attrs={"class": "review-micro"})
    if not review:
        return
    else:
        return {
            "letterboxd_movie_id": movie_id,
            "interaction_type": Interactions.REVIEW.value,
            "letterboxd_username": user_id,
        }


async def generate_ratings_operations(response, send_to_db=True, return_unrated=False):
    # Parse ratings page response for each rating/review, use lxml parser for speed
    soup = BeautifulSoup(response[0], "lxml")
    reviews = soup.findAll("li", attrs={"class": "poster-container"})

    # Create empty array to store list of bulk operations or rating objects
    ratings_operations = []
    movie_operations = []

    # For each review, parse data from scraped page and append an UpdateOne operation for bulk execution or a rating object
    for review in reviews:
        movie_id = review.find("div", attrs={"class", "film-poster"})["data-target-link"].split(
            "/"
        )[-2]
        user_id = response[1]["username"]

        rating_object = get_user_rating_interaction(review, movie_id, user_id)
        review_object = get_reviewed_interaction(review, movie_id, user_id)
        like_object = get_like_interaction(review, movie_id, user_id)
        watch_object = {
            "letterboxd_movie_id": movie_id,
            "interaction_type": Interactions.WATCHED.value,
            "letterboxd_username": user_id,
        }

        interaction_list = [rating_object, review_object, like_object, watch_object]

        # We're going to eventually send a bunch of upsert operations for movies with just IDs
        # For movies already in the database, this won't impact anything
        # But this will allow us to easily figure out which movies we need to scraped data on later,
        # Rather than scraping data for hundreds of thousands of movies everytime there's a broader data update
        skeleton_movie_object = {"letterboxd_movie_id": movie_id}

        for interaction in interaction_list:
            if interaction:
                ratings_operations.append(
                    UpdateOne(
                        {
                            "letterboxd_username": user_id,
                            "letterboxd_movie_id": movie_id,
                            "interaction_type": interaction["interaction_type"],
                        },
                        {"$set": interaction},
                        upsert=True,
                    )
                )

        movie_operations.append(
            UpdateOne(
                {"letterboxd_movie_id": movie_id},
                {"$set": skeleton_movie_object},
                upsert=True,
            )
        )

    return ratings_operations, movie_operations


async def get_user_ratings(
    username,
    db_cursor=None,
    mongo_db=None,
    store_in_db=True,
    num_pages=None,
    return_unrated=False,
    retries=3,
):
    url = "https://letterboxd.com/{}/films/by/date/page/{}/"

    if not num_pages:
        # Find them in the MongoDB database and grab the number of ratings pages
        user = db_cursor.find_one({"username": username})

        # We're trying to limit the number of pages we crawl instead of wasting tons of time on
        # gathering ratings we've already hit (see comment in get_page_counts)
        num_pages = user["recent_page_count"]

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    urls = []

    for i in range(num_pages):
        urls.append(url.format(username, i + 1))

    results = [None] * len(urls)
    failed_urls = urls.copy()

    for retry in range(retries):
        try:
            async with ClientSession() as session:
                if len(failed_urls) < len(urls):
                    print(f"\nRetrying {len(failed_urls)} urls\n")
                tasks = [
                    asyncio.ensure_future(fetch(url, session, {"username": username}))
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

    scrape_responses = [x for x in results if x]

    # Process each ratings page response, converting it into bulk upsert operations or output dicts
    tasks = []
    for response in scrape_responses:
        task = asyncio.ensure_future(
            generate_ratings_operations(
                response, send_to_db=store_in_db, return_unrated=return_unrated
            )
        )
        tasks.append(task)

    parse_responses = await asyncio.gather(*tasks)

    if store_in_db == False:
        parse_responses = list(chain.from_iterable(list(chain.from_iterable(parse_responses))))
        return parse_responses

    # Concatenate each response's upsert operations/output dicts
    upsert_ratings_operations = []
    upsert_movies_operations = []
    for response in parse_responses:
        upsert_ratings_operations += response[0]
        upsert_movies_operations += response[1]

    return upsert_ratings_operations, upsert_movies_operations


async def get_ratings(usernames, db_cursor=None, mongo_db=None, store_in_db=True):
    ratings_collection = mongo_db["letterboxd_interactions"]
    movies_collection = mongo_db["letterboxd_movies"]

    chunk_size = 10
    total_chunks = math.ceil(len(usernames) / chunk_size)
    users_pbar = tqdm(range(total_chunks), position=1)

    for chunk_index in users_pbar:
        tasks = []
        db_ratings_operations = []
        db_movies_operations = []

        start_index = chunk_size * chunk_index
        end_index = chunk_size * chunk_index + chunk_size
        username_chunk = usernames[start_index:end_index]

        users_pbar.set_description(
            f"Gathering ratings data for batch {chunk_index+1} of {total_chunks}"
        )

        # pbar.set_description(f"Scraping ratings data for user group {chunk_index+1} of {total_chunks}")

        # For a given chunk, scrape each user's ratings and form an array of database upsert operations
        for i, username in enumerate(username_chunk):
            # print((chunk_size*chunk_index)+i, username)
            task = asyncio.ensure_future(
                get_user_ratings(
                    username,
                    db_cursor=db_cursor,
                    mongo_db=mongo_db,
                    store_in_db=store_in_db,
                )
            )
            tasks.append(task)

        # Gather all ratings page responses, concatenate all db upsert operatons for use in a bulk write
        user_responses = await asyncio.gather(*tasks)
        for response in user_responses:
            db_ratings_operations += response[0]
            db_movies_operations += response[1]

        if store_in_db:
            # Execute bulk upsert operations
            try:
                batch_size = 10000
                if len(db_ratings_operations) > 0:
                    # Bulk write all upsert operations into ratings collection in db
                    # ratings_collection.bulk_write(db_ratings_operations, ordered=False)
                    num_batches = math.ceil(len(db_ratings_operations) / batch_size)

                    for chunk in range(num_batches):
                        ratings_set = db_ratings_operations[
                            chunk * batch_size : (chunk + 1) * batch_size
                        ]
                        ratings_collection.bulk_write(ratings_set, ordered=False)

                if len(db_movies_operations) > 0:
                    # movies_collection.bulk_write(db_movies_operations, ordered=False)
                    num_batches = math.ceil(len(db_movies_operations) / batch_size)

                    for chunk in range(num_batches):
                        movies_set = db_movies_operations[
                            chunk * batch_size : (chunk + 1) * batch_size
                        ]

                        movies_collection.bulk_write(movies_set, ordered=False)

            except BulkWriteError as bwe:
                pprint(bwe.details)


def main():
    # Connect to MongoDB client
    db_name, client = connect_to_database()

    # Find letterboxd database and user collection
    db = client[db_name]
    users = db.letterboxd_users

    # Starting to attach last_updated times, so we can cycle though updates instead of updating every user's
    # ratings every time. We'll just grab the 1000 records which are least recently updated
    # change to sort ascending i.e 1
    all_users = list(users.find({}).sort("last_updated", 1).limit(1000))
    all_usernames = [x["username"] for x in all_users]

    # chunking up processing so we can do it in parallel
    large_chunk_size = 100
    num_chunks = math.ceil(len(all_usernames) / large_chunk_size)

    pbar = tqdm(range(num_chunks), position=0)
    for chunk in pbar:
        pbar.set_description(f"Scraping ratings data for user group {chunk+1} of {num_chunks}")
        username_set = all_usernames[chunk * large_chunk_size : (chunk + 1) * large_chunk_size]

        loop = asyncio.get_event_loop()

        # Find number of ratings pages for each user and add to their Mongo document (note: max of 128 scrapable pages)
        future = asyncio.ensure_future(get_page_counts(username_set, users))
        # future = asyncio.ensure_future(get_page_counts([], users))
        loop.run_until_complete(future)

        # Find and store ratings for each user
        future = asyncio.ensure_future(get_ratings(username_set, users, db))
        # future = asyncio.ensure_future(get_ratings(["samlearner"], users, db))
        loop.run_until_complete(future)


if __name__ == "__main__":
    main()
