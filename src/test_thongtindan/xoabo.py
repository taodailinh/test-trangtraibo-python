from pymongo import MongoClient

client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
db = client["quanlytrangtrai_1109"]
boNhapTrai = db["BoNhapTrai"]

boNhapTrai.delete_one({"SoTai": "F202300609"})
print("Đã xóa thành công")
