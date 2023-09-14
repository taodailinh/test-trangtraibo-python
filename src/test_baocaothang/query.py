from pymongo import MongoClient
from datetime import datetime
import time
from .settings import settings as settings


startTime = time.time()
# Kết nối db
client = MongoClient(settings.connectionString)
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
db = client[settings.db]
# db = client["quanlytrangtrai_0910"]
khamThai = db["KhamThai"]


def printAllKetQuaKhamThai():
    ketquakhamthai = khamThai.distinct("KetQuaKham")
    for ketqua in ketquakhamthai:
        print(ketqua)


def soLuongBoKham(startdate, enddate):
    date_format = "%Y-%m-%d"
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "NgayKham": {"$gte": startDate, "$lte": endDate},
                "KetQuaKham": "Có thai",
            }
        },
        {
            "$group": {
                "_id": "null",
                "total": {"$count": {}},
            }
        },
    ]
    soLuongBoKhamThai = khamThai.aggregate(pipeline)
    for bo in soLuongBoKhamThai:
        print("Số lượng bò:" + str(bo.get("total")))


def soLuongBoKhamPhoiLan1(startdate, enddate):
    date_format = "%Y-%m-%d"
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$lookup": {
                "from": "inventory",
                "localField": "item",
                "foreignField": "sku",
                "as": "inventory_docs",
            }
        },
        {
            "$match": {
                "NgayKham": {"$gte": startDate, "$lte": endDate},
                "KetQuaKham": "Có thai",
            }
        },
        {
            "$group": {
                "_id": "null",
                "total": {"$count": {}},
            }
        },
    ]
    soLuongBoKhamThai = khamThai.aggregate(pipeline)
    for bo in soLuongBoKhamThai:
        print("Số lượng bò:" + str(bo.get("total")))


"""
def printNongTruongBCT():
    pipeline = [
        {"$match": {"LoaiDongCo": "BanChanTha"}},
        {
            "$group": {
                "_id": "$TenNongTruongCo",
                "total": {"$count": {}},
            }
        },
    ]
    nongTruongBCT = nongTruongCo.aggregate(pipeline)
    for nongTruong in nongTruongBCT:
        print(nongTruong)


def calArea():
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
    dienTichTungNongTruong = list(nongTruongCo.aggregate(pipeline))
    for nongTruong in dienTichTungNongTruong:
        print(nongTruong.DienTich)


# Tính lượng phân đã đưa vào luống (Đã xác nhận)
def tinhTongPhanDuaVaoLuong():
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
    results = sanXuanPhan.aggregate(pipeline)
    for result in results:
        print(result)


# Tính lượng phân đã thu hoạch (Đã xác nhận)
def tinhTongPhanDaThuHoach():
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
    results = sanXuanPhan.aggregate(pipeline)
    for result in results:
        khoiLuong = result.get("KhoiLuong") + 1
        print(khoiLuong)


def printAllDistintValueOfAField(fieldName):
    for value in sanXuanPhan.distinct(fieldName):
        print(value)
"""

beforeQuery = time.time()
printAllKetQuaKhamThai()
soLuongBoKham("2023-09-01", "2023-09-13")
endTime = time.time()
executionTime = endTime - startTime
queryTime = endTime - beforeQuery
print("Tong thoi gian:" + str(executionTime))
print("Thoi gian query:" + str(queryTime))
