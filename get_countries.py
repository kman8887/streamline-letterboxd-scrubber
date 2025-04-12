import json
import csv


def get_countries_json():
    with open(
        "/Users/garethmiskimmin/streamline-app/streamline-letterboxd-scrubber/countries.json", "r"
    ) as file:
        countries = json.load(file)
        countries = [
            ({"code": country["iso_3166_1"], "name": country["native_name"]})
            for country in countries
        ]

        countries = sorted(countries, key=lambda x: x["code"])

        return countries


def write_countries_csv(country_list):
    with open("countries.csv", "w", newline="") as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=";", quotechar="|", quoting=csv.QUOTE_MINIMAL)
        for country in country_list:
            spamwriter.writerow([country["name"], country["code"]])


if __name__ == "__main__":
    countries = get_countries_json()
    print(countries)
    write_countries_csv(countries)
