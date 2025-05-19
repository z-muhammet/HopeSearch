from pymongo import MongoClient, errors
from bson import ObjectId

class MongoDbContext:
    def __init__(self, url, db_name):
        self.url = url
        self.db_name = db_name

    def get_datas_from_mongodb(self, collection_name, query={}, limit=0):
        client = MongoClient(self.url)
        database = client[self.db_name]
        collection = database[collection_name]
        cursor = collection.find(query)
        if limit > 0:
            cursor = cursor.limit(limit)
        records = list(cursor)
        client.close()
        return records

    def save_datas_to_mongo(self, collection_name, datas):
        client = MongoClient(self.url)
        database = client[self.db_name]
        collection = database[collection_name]

        try:
            if isinstance(datas, list):
                collection.insert_many(datas)
            elif isinstance(datas, dict):
                try:
                    collection.insert_one(datas)
                except errors.DuplicateKeyError:
                    print(f"[WARN] _id çakışması: {datas['_id']} -> Yeni _id ile eklenecek.")
                    datas["_id"] = ObjectId()
                    collection.insert_one(datas)
            else:
                raise ValueError("Unsupported data type for saving to mongo")

            print("datas saved to:", collection_name)

        finally:
            client.close()

    def update_mongo_record(self, collection_name, query, update_data):
        client = MongoClient(self.url)
        database = client[self.db_name]
        collection = database[collection_name]
        result = collection.update_one(query, update_data)
        client.close()
        return result
