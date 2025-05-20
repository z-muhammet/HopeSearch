from typing import Any, Dict, List
from .mongo_context import MongoDbContext

class Repository:
    """
    Bir MongoDbContext ve koleksiyon ismi alır,
    CRUD işlemlerini bu ikili üzerinden kolaylaştırır.
    """
    def __init__(self, collection_name: str, context: MongoDbContext):
        self.collection = collection_name
        self.ctx = context

    def get(self, query: Dict[str, Any], limit: int = 0) -> List[Dict[str, Any]]:
        return self.ctx.get_datas_from_mongodb(self.collection, query, limit)

    def save(self, data: Dict[str, Any] or List[Dict[str, Any]]):
        return self.ctx.save_datas_to_mongo(self.collection, data)

    def update(self, filter_query: Dict[str, Any], update_query: Dict[str, Any]):
        return self.ctx.update_mongo_record(self.collection, filter_query, update_query)

    def delete(self, filter_query: Dict[str, Any]):
        return self.ctx.delete_from_mongo(self.collection, filter_query)
