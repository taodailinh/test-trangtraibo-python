from pymongo import MongoClient


def xoaBo(client: MongoClient, dbName, collectionName, soTai):
    db = client[dbName]
    boNhapTrai = db[collectionName]

    boNhapTrai.delete_one({"SoTai": soTai})
    print("Đã xóa thành công")
