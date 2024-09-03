import os
import argparse
from datetime import datetime, timedelta, UTC

from dotenv import load_dotenv
from pymongo import MongoClient
from tqdm import tqdm

from knackly_api import KnacklyAPI
from mongo_db import (
    format_document,
    copy_created_dates,
    copy_app_user_types,
    copy_catalog,
)
from logger import initialize_logger


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
            help="specify a date in the format `YYYY-MM-DD`. Any records with a lastModified date greater than this date will be what is searched. Defaults to the first of the previous month.",
        )
        return parser

    parser = init_argparse()
    args = parser.parse_args()

    # Validate that args.date is in the format YYYY-MM-DD.
    if args.date:
        try:
            args.date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            parser.error(
                f"please ensure that the date is in the format YYYY-MM-DD. received: {args.date}"
            )
    else:
        # If args.date was not provided, default it to be the first day of the previous month
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        first_of_the_month = today.replace(day=1)
        first_of_last_month = (first_of_the_month - timedelta(days=1)).replace(day=1)
        args.date = first_of_last_month

    # Once args.date is validated, convert it back into a string
    args.date = datetime.strftime(args.date, "%Y-%m-%d")

    return args


def main(args: argparse.Namespace):
    log = initialize_logger()
    # Setup API credentials
    load_dotenv()
    knackly = KnacklyAPI(
        key_id=os.getenv("KEY"),
        secret=os.getenv("SECRET"),
        tenancy=os.getenv("TENANCY"),
    )
    mongo_user = os.getenv("MONGO_USER")
    mongo_pass = os.getenv("MONGO_PASSWORD")
    mongo_cluster = os.getenv("MONGO_CLUSTER")
    client = MongoClient(
        f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_cluster}/?retryWrites=true&w=majority"
    )
    db = client["LightningDocs"]
    collection = db["rob_test_Records"]

    # Get a list of all catalogs available to this API key
    catalog_objects = knackly.get_available_catalogs()
    catalogs = [c["name"] for c in catalog_objects if "name" in c]

    # Build a dictionary consisting of the key:value pairs record_id:record
    # (record is itself a dictionary, containing metadata about the record and what catalog it came from)
    log.debug(f"Collecting record metadata across {len(catalogs)} catalogs...")
    record_id_map = {}
    pbar = tqdm(enumerate(catalogs), total=len(catalogs))
    for idx, c in pbar:
        pbar.set_description(str(c).ljust(15))
        if idx == 3:
            # break
            pass

        records = knackly.get_records_in_catalog(
            catalog=c,
            status="Ok",
            limit=1000,
            last_modified={"c": "after", "v": f"{args.date}T00:00"},
        )

        # Break out of this iteration if there weren't any records found matching the various filters.
        if len(records) == 0:
            continue

        # Inject the catalog into the metadata about each record, and then update record_id_map
        for r in records:
            r.update({"catalog": c})
        record_id_map.update({r["id"]: r for r in records if "id" in r})

    # Using the record_id_map, create a subset for id's already in mongodb and a subset for id's not in mongodb.
    record_ids = [r for r in record_id_map]
    record_ids_set = set(record_ids)
    matching_docs = collection.find({"id": {"$in": record_ids}}, {"id": 1})

    matching_ids = {document["id"] for document in matching_docs if "id" in document}
    non_matching_ids = record_ids_set - matching_ids

    # For each non-matching id: add it to mongodb
    log.debug(f"Adding {len(non_matching_ids)} new documents to MongoDB...")
    if non_matching_ids:
        log.info(f"{'-'*63}")
        log.info(
            f"{str('Record id').ljust(23)} | {str('Catalog').ljust(20)} | Created Date"
        )
        log.info(f"{'-'*63}")
        for id in tqdm(non_matching_ids):
            catalog = record_id_map[id].get("catalog")
            created_date = record_id_map[id].get("created")

            record_details = knackly.get_record_details(id, catalog)
            document = format_document(
                record_details, catalog
            )  # Implementation of `format_document()` will change in the future
            result = collection.insert_one(document)
            log.info(f"{str(id).ljust(23)} | {str(catalog).ljust(20)} | {created_date}")
    log.info(
        f"{len(non_matching_ids)} id's found in Knackly that don't currently exist in MongoDB."
    )

    # For each matching id: check if it was modified past what we have stored in mongodb
    matching_ids_metadata = [
        r for r in record_id_map.values() if r.get("id") in matching_ids
    ]
    log.debug(
        f"Searching through {len(matching_ids)} existing id's for outdated documents to replace..."
    )
    modified_document_count = 0
    heading_already_printed = False
    pbar = tqdm(matching_ids_metadata)
    for r in pbar:
        pbar.set_description(f"{modified_document_count} documents replaced")
        mongo_document = collection.find_one({"id": r.get("id")})
        knackly_last_modified = datetime.strptime(
            r.get("lastModified"), "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        mongo_last_modified = datetime.strptime(
            mongo_document.get("lastModified"), "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        if knackly_last_modified > (mongo_last_modified + timedelta(minutes=5)):
            # Modify the document to make it conform to what MongoDB expects.
            record_details = knackly.get_record_details(
                r.get("id"), catalog=r.get("catalog")
            )
            document = copy_catalog(mongo_document, record_details)
            document = copy_created_dates(mongo_document, document)
            document = copy_app_user_types(mongo_document, document)
            result = collection.replace_one(
                filter={"id": record_details.get("id")}, replacement=document
            )
            modified_document_count += 1

            # Log the heading information for this section
            if not heading_already_printed:
                heading_already_printed = True
                log.info(f"{'-'*117}")
                log.info(
                    f"{str('Record id').ljust(23)} | {str('Catalog').ljust(20)} | {'Knackly last modified'.ljust(26)} | {'Mongo last modified'.ljust(26)} | Difference"
                )
                log.info(f"{'-'*117}")
            log.info(
                f"{r.get('id').ljust(23)} | {r.get('catalog').ljust(20)} | {str(knackly_last_modified).ljust(26)} | {str(mongo_last_modified).ljust(26)} | {knackly_last_modified - mongo_last_modified}"
            )
    log.info(
        f"{modified_document_count} out of the {len(matching_ids)} matching documents were replaced with their latest versions."
    )


if __name__ == "__main__":
    args = parse_arguments()
    main(args)
