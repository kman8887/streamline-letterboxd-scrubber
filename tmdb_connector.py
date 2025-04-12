from aiohttp import ClientSession
import asyncio
from pymongo.database import Database
from tqdm import tqdm
from pymongo.errors import BulkWriteError
from ratelimit import limits, sleep_and_retry


@sleep_and_retry
@limits(calls=50, period=1)
async def fetch_json(url, session: ClientSession, retries=3):
    attempt = 0
    while attempt < retries:
        try:
            async with session.get(url) as response:
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


async def fetch_and_store(urls: list, database: Database, chunk_size: int, fetch_method):
    async with ClientSession() as session:
        for i in tqdm(range(0, len(urls), chunk_size)):
            chunk = urls[i : i + chunk_size]
            tasks = [fetch_method(url, session) for url in chunk]

            results = await asyncio.gather(*tasks)

            # Filter out failed requests (None values)
            updates = [r for r in results if r]
            if updates:
                try:
                    database.bulk_write(updates, ordered=False)
                except BulkWriteError as bwe:
                    print("MongoDB Error:", bwe.details)
