from pymongo import MongoClient
from datetime import datetime
import time


def printAllNongTruong(
    client: MongoClient, databaseName, collectionName="NongTruongCo"
):
    dbase = client[databaseName]
    col = dbase[collectionName]
    nongTruongCos = col.distinct("TenNongTruongCo")
    for nongTruong in nongTruongCos:
        print(nongTruong)


def printNongTruongBCT(client: MongoClient, db, collection="NongTruongCo"):
    pipeline = [
        {"$match": {"LoaiDongCo": "BanChanTha"}},
        {
            "$group": {
                "_id": "$TenNongTruongCo",
                "total": {"$count": {}},
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    nongTruongBCT = col.aggregate(pipeline)
    for nongTruong in nongTruongBCT:
        print(nongTruong)


def calArea(client: MongoClient, db, collection="NongTruongCo"):
    pipeline = [
        # unwind lô cỏ
        {"$unwind": "$LoCos"},
        # unwind lô cỏ cũ
        {"$unwind": "$LoCos.LoCoCus"},
        # chỉ lấy những lô cỏ chưa quy hoạch
        {"$match": {"LoCos.LoCoCus.IsQuyHach": False}},
        # group lại theo nông trường
        {
            "$group": {
                "_id": "null",
                "TongDienTich": {"$sum": "$LoCos.LoCoCus.DienTichTrong"},
            }
        },
        # project ra document
        {
            "$project": {
                "_id": 0,
                "NongTruong": "$TenNongTruongCo",
                "DienTich": "$TongDienTich",
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    dienTichTungNongTruong = list(col.aggregate(pipeline))
    for nongTruong in dienTichTungNongTruong:
        print(nongTruong.DienTich)


# Tính lượng phân đã đưa vào luống (Đã xác nhận)
def tinhTongPhanDuaVaoLuong(
    client: MongoClient, db, collection="PhieuCongViecLuongPhan"
):
    startDate = datetime(2023, 9, 1)
    endDate = datetime(2023, 9, 13)
    pipeline = [
        # Giới hạn ngày giờ
        {
            "$match": {
                "NgayThucHien": {
                    "$gte": startDate,
                    "$lte": endDate,
                },
                "LoaiCongViec": "DuaPhanVaoLuong",
                "XacNhanCongViec": "DaXacNhan",
            }
        },
        # group lại theo nông trường
        {
            "$group": {
                "_id": "null",
                "TongKhoiLuongDaDua": {"$sum": "$SoLuong"},
            }
        },
        # project ra document
        {
            "$project": {
                "_id": 0,
                "KhoiLuong": "$TongKhoiLuongDaDua",
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    results = col.aggregate(pipeline)
    for result in results:
        print(result)


# Tính lượng phân đã thu hoạch (Đã xác nhận)
def tinhTongPhanDaThuHoach(
    client: MongoClient, db, collection="PhieuCongViecLuongPhan"
):
    startDate = datetime(2023, 8, 13)
    endDate = datetime(2023, 9, 13)
    pipeline = [
        # Giới hạn ngày giờ
        {
            "$match": {
                "NgayThucHien": {
                    "$gte": startDate,
                    "$lte": endDate,
                },
                "LoaiCongViec": "ThuHoach",
            }
        },
        # group lại theo nông trường
        {
            "$group": {
                "_id": "null",
                "TongKhoiLuongDaThuHoach": {"$sum": "$SoLuong"},
            }
        },
        # project ra document
        {
            "$project": {
                "_id": 0,
                "KhoiLuong": "$TongKhoiLuongDaThuHoach",
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    results = col.aggregate(pipeline)
    for result in results:
        khoiLuong = result.get("KhoiLuong") + 1
        print(khoiLuong)


def printAllDistintValueOfAField(
    client: MongoClient,
    db,
    fieldName,
    collection="PhieuCongViecLuongPhan",
):
    dbase = client[db]
    col = dbase[collection]
    for value in col.distinct(fieldName):
        print(value)
