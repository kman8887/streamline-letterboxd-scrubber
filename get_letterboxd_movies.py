import asyncio
import datetime
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from tqdm import tqdm
from database import connect_to_database
import time

CHUNK_SIZE = 50


async def fetch_letterboxd(url, session, movie_id, retries=3):
    """Fetch movie details from Letterboxd asynchronously."""
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "lxml")

                # Extract movie details
                movie_header = soup.find("section", attrs={"class": "film-header-group"})
                try:
                    movie_title = movie_header.find("h1").text
                except AttributeError:
                    movie_title = ""

                try:
                    year = int(
                        movie_header.find("div", attrs={"class": "releaseyear"}).find("a").text
                    )
                except AttributeError:
                    year = None

                try:
                    imdb_link = soup.find("a", attrs={"data-track-action": "IMDb"})["href"]
                    imdb_id = imdb_link.split("/title")[1].strip("/").split("/")[0]
                except:
                    imdb_link = ""
                    imdb_id = ""

                try:
                    tmdb_link = soup.find("a", attrs={"data-track-action": "TMDb"})["href"]
                    tmdb_id = tmdb_link.split("/movie")[1].strip("/").split("/")[0]
                except:
                    tmdb_link = ""
                    tmdb_id = ""

                movie_object = {
                    "movie_title": movie_title,
                    "year_released": year,
                    "imdb_link": imdb_link,
                    "tmdb_link": tmdb_link,
                    "imdb_id": imdb_id,
                    "tmdb_id": tmdb_id,
                }

                return UpdateOne(
                    {"letterboxd_movie_id": movie_id},
                    {"$set": movie_object},
                    upsert=True,
                )
        except Exception as e:
            attempt += 1
            print(f"\nAttempt {attempt} failed for URL {url} with exception: {repr(e)}\n")
            if attempt == retries:
                print(f"\nMax retries reached for URL {url}\n")
                return None
            await asyncio.sleep(2)


async def scrape_movies(movie_list, db):
    """Scrapes movies in chunks to avoid overload."""
    url_template = "https://letterboxd.com/film/{}/"

    async with ClientSession() as session:
        for i in tqdm(range(0, len(movie_list), CHUNK_SIZE)):
            chunk = movie_list[i : i + CHUNK_SIZE]

            # Create tasks for all movies in the chunk
            start_fetch = time.time()

            tasks = [
                fetch_letterboxd(url_template.format(movie), session, movie) for movie in chunk
            ]

            # Run tasks concurrently and gather results
            results = await asyncio.gather(*tasks)

            fetch_time = time.time() - start_fetch
            print(f"Fetching {len(chunk)} URLs took {fetch_time:.2f} seconds")

            # Filter out failed requests (None values)
            updates = [r for r in results if r]

            start_db = time.time()
            # Store in MongoDB
            if updates:
                try:
                    db["letterboxd_movies"].bulk_write(updates, ordered=False)
                except BulkWriteError as bwe:
                    print("MongoDB Error:", bwe.details)

            db_time = time.time() - start_db
            print(f"Database update took {db_time:.2f} seconds\n")


async def main():
    """Main function to handle database connection and scraping process."""
    db_name, client = connect_to_database()
    db = client[db_name]

    # Get list of movies that need to be scraped
    movies_to_scrape = [
        x["letterboxd_movie_id"]
        for x in db["letterboxd_movies"].find({"tmdb_id": {"$exists": False}})
    ]

    print(f"Scraping {len(movies_to_scrape)} movies in chunks of {CHUNK_SIZE}...")

    await scrape_movies(movies_to_scrape, db)


if __name__ == "__main__":
    asyncio.run(main())
