import requests
import json


class KnacklyAPI:
    def __init__(self, key_id: str, secret: str, tenancy: str):
        self.key_id = key_id
        self.secret = secret
        self.tenancy = tenancy
        self.base_url = f"https://api.knackly.io/{tenancy}/api/v1"
        self.authorization_header = {
            "Authorization": f"Bearer {self.get_access_token() }"
        }
        print("Successfully connected to Knackly.")

    def get_access_token(self) -> str:
        """Get an access token needed for other Knackly API requests.

        Returns:
            str: A long string that will act as the access token for further API requests.
        """
        url = f"{self.base_url}/auth/login"
        payload = {"KeyID": self.key_id, "Secret": self.secret}
        r = requests.post(url, data=payload)
        return r.json()["token"]

    def get_available_catalogs(self) -> list[dict]:
        """Lists the catalogs available to the api key.

        Returns:
            list[dict]: A list of catalog dictionaries containing metadata about each catalog.
        """
        url = f"{self.base_url}/catalogs"
        response = requests.get(url, headers=self.authorization_header)

        response.raise_for_status()
        return response.json()

    def get_records_in_catalog(
        self,
        catalog: str,
        status: str = None,
        last_modified: dict = None,
        skip: int = 0,
        limit: int = 20,
    ) -> list[dict]:
        """List the metadata about records in a given catalog

        Args:
            catalog (str): Name of the catalog
            status (str): Status filter of the record. Can be either `Ok` or `Needs Updating`. Defaults to None.
            last_modified (dict): Dictionary describing the filter to apply to the last modified date. see below for examples. Defaults to None.
            skip (int, optional): Offset of first listed item. Defaults to 0.
            limit (int, optional): Page size. Defaults to 20.

        Returns:
            list[dict]: Metadata about various records in a given catalog that match the search parameters

        last_modified Examples:
            {"c":"after","v":"2024-07-01T00:00"}
            {"c":"range","dateStart":"2024-07-02T11:09","dateEnd":"2024-07-03T11:09"}
            {"c":"before","v":"2024-07-03T11:00"}
        """
        url = f"{self.base_url}/catalogs/{catalog}/items"

        if last_modified:
            lastmod_dict = {"lastmod": last_modified}
            lastmod_str = json.dumps(lastmod_dict, separators=(",", ":"))

        params = {
            "status": status,
            "skip": skip,
            "limit": limit,
            "f": lastmod_str,
        }

        # Remove any None values from params
        params = {k: v for k, v in params.items() if v is not None}

        response = requests.get(url, headers=self.authorization_header, params=params)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(f"Error: {e}")
            print("Response content:", response.content)
            print("Response URL:", response.url)
            raise

        result = response.json()
        return result

    def get_record_details(self, record_id: str, catalog: str) -> dict:
        """Query's the Knackly API for information regarding a specific record

        Args:
            record_id (str): The unique id of the record, typically gotten from a webhook event firing
            catalog (str): The catalog that the record resides in

        Returns:
            dict: A python object containing information about the record
        """
        url = f"{self.base_url}/catalogs/{catalog}/items/{record_id}"
        r = requests.get(url, headers=self.authorization_header)
        if r.status_code == 400 or r.status_code == 403:
            raise RuntimeError(
                f"{r.status_code}: something went wrong while trying to get {record_id} in {catalog}: {r.text}"
            )
        return r.json()

    def pretty_print_request_details(self, req: requests.Request) -> None:
        """Helper function to print out the full information that python is sending to the server

        Args:
            req (requests.Request): A python Request object
        """
        print(
            "{}\n{}\r\n{}\r\n\r\n{}".format(
                "-----------START-----------",
                req.method + " " + req.url,
                "\r\n".join("{}: {}".format(k, v) for k, v in req.headers.items()),
                req.body,
            )
        )
