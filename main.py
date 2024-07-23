import os

from dotenv import load_dotenv
from pymongo import MongoClient

from knackly_api import KnacklyAPI


def main():
    load_dotenv()

    knackly = KnacklyAPI(
        key_id=os.getenv("KEY"),
        secret=os.getenv("SECRET"),
        tenancy=os.getenv("TENANCY"),
    )

    last_modified = {"c": "after", "v": "2024-07-17T00:00"}
    x = knackly.get_records_in_catalog(
        catalog="Transactional", last_modified=last_modified, limit=3, status="Ok"
    )
    print(type(x))
    print(len(x))

    x = knackly.get_available_catalogs()
    print(type(x))
    print(len(x))
    print(x[0])
    print(type(x[0]))


if __name__ == "__main__":
    main()
