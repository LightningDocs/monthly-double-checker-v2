def format_document(record_details: dict, catalog: str) -> dict:
    """Formats a raw Knackly record into the MongoDB structure that we expect.

    Args:
        record_details (dict): The results from calling the `get_record_details` method from the KnacklyAPI class.
        catalog (str): The name of the catalog that this record came from

    Returns:
        dict: The slightly modified Knackly record
    """
    # Inject the catalog as a new key/value
    record_details["LD_catalog"] = catalog

    for app in record_details.get("apps"):
        # Copy the created date from the record into a new key/value pair for each app
        app["LD_createdDate"] = record_details.get("created")

        # Create a new key/value pair for the user type. Assume it was 'api' if the user that last ran the app ends with '_api', otherwise assume 'regular'
        app["LD_userType"] = (
            "api" if record_details.get("lastRunBy", "").endswith("_api") else "regular"
        )

    return record_details


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


def main():
    pass


if __name__ == "__main__":
    main()
