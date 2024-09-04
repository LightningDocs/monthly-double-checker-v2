from datetime import datetime, UTC
from pymongo.collection import Collection

from knackly_api import guess_responsible_app


def format_document(record_details: dict, catalog: str) -> dict:
    """Formats a raw Knackly record into the MongoDB structure that we expect.

    Args:
        record_details (dict): The results from calling the `get_record_details` method from the KnacklyAPI class.
        catalog (str): The name of the catalog that this record came from

    Returns:
        dict: The slightly modified Knackly record
    """

    def fill_billing_array(apps: list) -> list:
        """Produces a list of billing objects based on a list of apps.

        Args:
            apps (list): A list of objects, where each object corresponds to a single app object.

        Returns:
            list: A list of objects containing an "app" key (string value) and a "billed" key (boolean/None value).
        """
        return [{"app": a["name"], "billed": None} for a in apps]

    document = {
        "catalog": catalog,
        "timeline": [],
        "billing": [],
        "record_id": record_details.get("id"),
        "internally_modified": datetime.now(UTC),
    }

    # Come up with an educated guess for what the responsible_app should be
    responsible_app = guess_responsible_app(record_details["apps"])

    # Inject the responsible_app into the record details,
    # and then inject the record_details into the timeline array.
    record_details["responsible_app"] = responsible_app
    document["timeline"].append(record_details)

    # Fill the billing array (potentially)
    data = record_details["data"]
    if not data.get("isTestFile"):
        document["billing"] = fill_billing_array(record_details["apps"])

    return document


def add_to_timeline(
    col: Collection,
    record_id: str,
    record_details: dict,
) -> None:
    """Adds the details of a record (as returned by Knackly API) to a timeline object for a particular document in MongoDB

    Args:
        col_name (collection): The pymongo collection object
        record_id (str): The id of the particular record
        record_details (dict): The record_details to be used to inject into the timeline
    """

    responsible_app = guess_responsible_app(record_details["apps"])
    record_details["responsible_app"] = responsible_app

    # If a document with the provided record_id cannot be found, throw an error
    result = col.find_one_and_update(
        {"record_id": record_id}, {"$push": {"timeline": record_details}}
    )
    if not result:
        raise ReferenceError(
            f"could not find a document in {col.full_name} with the record id: {record_id}"
        )


def update_internally_modified(col: Collection, record_id: str) -> None:
    """Updates the internally modified field of a record

    Args:
        col (Collection): The pymongo collection object
        record_id (str): The id of the particular record
    """
    result = col.find_one_and_update(
        {"record_id": record_id}, {"$currentDate": {"internally_modified": True}}
    )
    if not result:
        raise ReferenceError(
            f"could not find a document in {col.full_name} with the record id: {record_id}"
        )


def main():
    pass


if __name__ == "__main__":
    main()
