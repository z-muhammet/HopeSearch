from pymongo import MongoClient
from config.settings import MONGO_URI, MONGO_DB_NAME

class MongoDbContext:
    def __init__(self, url: str = MONGO_URI, db_name: str = MONGO_DB_NAME):
        self.client = MongoClient(url)
        self.db = self.client[db_name]

    def get_datas_from_mongodb(self, collection_name: str, query: dict = {}, limit: int = 0):
        cursor = self.db[collection_name].find(query)
        if limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)

    def save_datas_to_mongo(self, collection_name: str, datas):
        if isinstance(datas, list):
            return self.db[collection_name].insert_many(datas)
        else:
            return self.db[collection_name].insert_one(datas)

    def update_mongo_record(self, collection_name: str, filter_query: dict, update_query: dict):
        return self.db[collection_name].update_one(filter_query, update_query)

    def delete_from_mongo(self, collection_name: str, filter_query: dict):
        return self.db[collection_name].delete_one(filter_query)
