from pymongo import MongoClient


def cowCount(collection, field, condition):
    return collection.count_documents({field: condition})
