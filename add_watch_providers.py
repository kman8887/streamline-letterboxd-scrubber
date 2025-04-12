import asyncio
import os
import psycopg
from database import load_config
from tmdb_connector import fetch_json
from aiohttp import ClientSession

TMDB_KEY = os.getenv("TMDB_KEY")


async def add_watch_providers():
    base_url = "https://api.themoviedb.org/3/watch/providers/movie?language=en-GB&api_key={}"

    async with ClientSession() as session:
        response = await fetch_json(base_url.format(TMDB_KEY), session)
        session.close()

    watch_providers_response = response["results"]

    watch_providers_data = [
        (provider["logo_path"], provider["provider_name"], provider["provider_id"])
        for provider in watch_providers_response
    ]

    watch_providers_region_priority_data = []
    for provider in watch_providers_response:
        for region, priority in provider["display_priorities"].items():
            watch_providers_region_priority_data.append(
                (provider["provider_id"], region, priority)  # provider_id  # region  # priority
            )

    insert_watch_providers_query = """
        INSERT INTO watch_providers (logo_path, provider_name, id)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """

    insert_watch_providers_region_priority_query = """
            INSERT INTO watch_provider_regions (provider_id, region, priority)
            VALUES (%s, %s, %s);
        """

    try:
        config = load_config()
        async with await psycopg.AsyncConnection.connect(config) as aconn:
            async with aconn.cursor() as acur:
                await acur.executemany(insert_watch_providers_query, watch_providers_data)
                await acur.executemany(
                    insert_watch_providers_region_priority_query,
                    watch_providers_region_priority_data,
                )
    finally:
        # Close the connection
        aconn.close()


if __name__ == "__main__":
    asyncio.run(add_watch_providers())
