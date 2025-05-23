from pymongo.operations import UpdateOne
import requests
from bs4 import BeautifulSoup

from pymongo.errors import BulkWriteError

from pprint import pprint
from tqdm import tqdm

from database import connect_to_database

# Connect to MongoDB client
db_name, client = connect_to_database()

db = client[db_name]
users = db["letterboxd_users"]

base_url = "https://letterboxd.com/members/popular/this/week/page/{}/"

total_pages = 128
pbar = tqdm(range(1, total_pages + 1))
for page in pbar:
    # Each page has 30 users therfore total_users = 30 * total_pages = 3840 users
    pbar.set_description(f"Scraping page {page} of {total_pages} of top users")

    r = requests.get(base_url.format(page))
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", attrs={"class": "person-table"})
    rows = table.findAll("td", attrs={"class": "table-person"})

    update_operations = []
    for row in rows:
        link = row.find("a")["href"]
        username = link.strip("/")
        display_name = row.find("a", attrs={"class": "name"}).text.strip()
        num_reviews = int(
            row.find("small").find("a").text.replace("\xa0", " ").split()[0].replace(",", "")
        )

        user = {"username": username, "display_name": display_name, "num_reviews": num_reviews}

        update_operations.append(
            UpdateOne({"username": user["username"]}, {"$set": user}, upsert=True)
        )

    try:
        if len(update_operations) > 0:
            users.bulk_write(update_operations, ordered=False)
    except BulkWriteError as bwe:
        pprint(bwe.details)
