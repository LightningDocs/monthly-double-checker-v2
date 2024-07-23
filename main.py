import os

from dotenv import load_dotenv
from pymongo import MongoClient

from knackly_api import KnacklyAPI


def main():
    load_dotenv()

    # knackly = KnacklyAPI(
    #     key_id=os.getenv("KEY"),
    #     secret=os.getenv("SECRET"),
    #     tenancy=os.getenv("TENANCY"),
    # )

    # last_modified = {"c": "after", "v": "2024-07-17T00:00"}
    # x = knackly.get_records_in_catalog(
    #     catalog="Transactional", last_modified=last_modified, limit=3, status="Ok"
    # )
    # print(type(x))
    # print(len(x))

    # x = knackly.get_available_catalogs()
    # print(type(x))
    # print(len(x))
    # print(x[0])
    # print(type(x[0]))

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

    ids_to_check = ["662a7f630cb5c188", "662a7630cba1df2", "123"]

    print("finding")
    matching_documents = collection.find({"id": {"$in": ids_to_check}}, {"id": 1})
    print("finished finding")

    matching_ids = [doc["id"] for doc in matching_documents]
    print("Matching IDs:", matching_ids)
    print("Matching documents:", matching_documents)


if __name__ == "__main__":
    main()
