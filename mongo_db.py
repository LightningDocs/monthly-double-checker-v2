from datetime import datetime, UTC
from pymongo import MongoClient

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


def copy_created_dates(previous_document: dict, new_document: dict) -> dict:
    """Copies the app.LD_createdDate attributes from a previous document into a new document.

    Args:
        previous_document (dict): A Knackly record-like document
        new_document (dict): A Knackly record-like document

    Returns:
        dict: The new, modified document.
    """
    date_map = {
        app["name"]: app.get("LD_createdDate") for app in previous_document["apps"]
    }

    # Iterate through the apps in the new dict
    for app in new_document["apps"]:
        # If the app name exists in the date_map, update the LD_creationDate in the new document
        if app["name"] in date_map and date_map[app["name"]] is not None:
            app["LD_createdDate"] = date_map[app["name"]]

    return new_document


def copy_app_user_types(previous_document: dict, new_document: dict) -> dict:
    """Copies the app.LD_userType attributes from a previous document into a new document.

    Args:
        previous_document (dict): A Knackly record-like document
        new_document (dict): A Knackly record-like document

    Returns:
        dict: The new, modified document.
    """
    user_type_map = {
        app["name"]: app.get("LD_userType") for app in previous_document.get("apps", {})
    }

    # Iterate through the apps in the new dict
    for app in new_document["apps"]:
        # If the app name exists in the date_map, update the LD_userType in the new document
        if app["name"] in user_type_map and user_type_map[app["name"]] is not None:
            app["LD_userType"] = user_type_map[app["name"]]

    return new_document


def copy_catalog(previous_document: dict, new_document: dict) -> dict:
    """Copies the LD_catalog key from a previous document into a new document

    Args:
        previous_document (dict): A Knackly record-like document
        new_document (dict): A Knackly record-like document

    Returns:
        dict: The new, modified document.
    """
    catalog = previous_document.get("LD_catalog")
    new_document["LD_catalog"] = catalog
    return new_document


def add_to_timeline(
    client: MongoClient,
    db_name: str,
    col_name: str,
    record_id: str,
    record_details: dict,
) -> None:
    """Adds the details of a record (as returned by Knackly API) to a timeline object for a particular record

    Args:
        client (MongoClient): The MongoClient object
        db_name (str): The name of the database to connect to
        col_name (str): The name of the collection to look in
        record_id (str): The id of the particular record
        record_details (dict): The record_details to be used to inject into the timeline
    """
    db = client[db_name]
    collection = db[col_name]

    responsible_app = guess_responsible_app(record_details["apps"])
    record_details["responsible_app"] = responsible_app

    # If a document with the provided record_id cannot be found, throw an error
    result = collection.find_one_and_update(
        {"record_id": record_id}, {"$push": {"timeline": record_details}}
    )

    if not result:
        raise ReferenceError(
            f"could not find a document in {db_name}.{col_name} with the record id: {record_id}"
        )


def main():
    pass


if __name__ == "__main__":
    main()
