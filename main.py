import os
import argparse
from datetime import datetime, timedelta, UTC

from dotenv import load_dotenv
from pymongo import MongoClient
from tqdm import tqdm

from knackly_api import KnacklyAPI


def parse_arguments() -> argparse.Namespace:
    """Helper function to parse command line arguments cleanly

    Returns:
        argparse.Namespace: Namespace object that should contain the `.exclude` property
    """

    def init_argparse() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-d",
            "--date",
            help="specify a date in the format `YYYY-MM-DD`. Any records with a lastModified date greater than this date will be ignored.",
        )
        return parser

    parser = init_argparse()
    args = parser.parse_args()

    # Validate that cutoff is a valid date in the format YYYY-MM-DD.
    if args.date:
        try:
            args.date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            parser.error(
                f"please ensure that the date is in the format YYYY-MM-DD. received: {args.date}"
            )
    else:
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        first_of_the_month = today.replace(day=1)
        first_of_last_month = (first_of_the_month - timedelta(days=1)).replace(day=1)
        args.date = first_of_last_month

    return args


def main(args: argparse.Namespace):
    load_dotenv()

    knackly = KnacklyAPI(
        key_id=os.getenv("KEY"),
        secret=os.getenv("SECRET"),
        tenancy=os.getenv("TENANCY"),
    )

    mongo_user = os.getenv("MONGO_USER")
    mongo_pass = os.getenv("MONGO_PASSWORD")
    mongo_cluster = os.getenv("MONGO_CLUSTER")
    print("connecting")
    client = MongoClient(
        f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_cluster}/?retryWrites=true&w=majority"
    )
    print("finished connecting")
    print()
    db = client["LightningDocs"]
    collection = db["Records"]

    catalog_objects = knackly.get_available_catalogs()
    catalogs = [c["name"] for c in catalog_objects if "name" in c]

    for idx, c in enumerate(catalogs):
        if idx == 3:
            # break
            pass

        records = knackly.get_records_in_catalog(
            catalog=c,
            status="Ok",
            limit=1000,
            last_modified={"c": "after", "v": "2024-07-01T00:00"},
        )

        # Break out of this iteration if there weren't any records found matching the various filters.
        if len(records) == 0:
            # print(f"{len(records)} records were found for {c} catalog, so skipping...")
            continue

        record_ids = [r["id"] for r in records if "id" in r]
        # print(record_ids)
        # print(
        #     f"{idx}. total records found in the {c} catalog: {len(record_ids)}. ",
        #     end="",
        # )
        matching_docs = collection.find({"id": {"$in": record_ids}}, {"id": 1})
        matching_ids = {
            document["id"] for document in matching_docs if "id" in document
        }  # This is a 'set' data structure.

        record_ids_set = set(record_ids)

        non_matching_ids = record_ids_set - matching_ids
        if len(non_matching_ids) == 0:
            continue

        print(
            f"{idx}. total records found in the {c} catalog: {len(record_ids)}. ",
            end="",
        )
        # print("Matching ids:", len(matching_ids))
        print("Non-matching ids:", len(non_matching_ids))

    # last_modified = {"c": "after", "v": "2024-07-17T00:00"}
    # x = knackly.get_records_in_catalog(
    #     catalog="Transactional", last_modified=last_modified, limit=3, status="Ok"
    # )
    # print(type(x))
    # print(len(x))

    # ids_to_check = ["662a7f630cb5c188", "662a7630cba1df2", "123"]

    # print("finding")
    # matching_documents = collection.find({"id": {"$in": ids_to_check}}, {"id": 1})
    # print("finished finding")

    # matching_ids = [doc["id"] for doc in matching_documents]
    # print("Matching IDs:", matching_ids)
    # print("Matching documents:", matching_documents)


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
