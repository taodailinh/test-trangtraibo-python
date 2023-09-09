from pymongo import MongoClient

serverlet = "mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/"
databaseName = "quanlytrangtrai_2807_clone"


def connectDb(serverlet, database, collection):
    client = MongoClient(serverlet)
    db = client[database]
    return db[collection]


def cowCount(collection, field, condition):
    return collection.count_documents({field: condition})


def printDistict(fieldName):
    boNhapTrai = connectDb(serverlet, databaseName, "BoNhapTrai")
    phanLoaiBo = boNhapTrai.distinct(fieldName)
    print(phanLoaiBo)


def printAllValueMeetCond(field, condition):
    boNhapTrai = connectDb(serverlet, databaseName, "BoNhapTrai")
    for bo in boNhapTrai.find({"PhanLoaiBo": "BoChoDe"}):
        print(bo["SoTai"])
