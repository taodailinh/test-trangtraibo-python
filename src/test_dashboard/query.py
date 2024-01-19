from pymongo import MongoClient
from datetime import datetime, timedelta
from bson.son import SON
import time
from client import db

date_format = "%Y-%m-%d"

def tongdanbo():
    pipeline = [
        {"$match":{
            "NhomBo":{"$in":["Bo","Be","BoChuyenVoBeo","BoDucGiong"]}
        }},
        {"$group":{
            "_id":"$PhanLoaiBo",
            "soluong":{"$count":{}},
            "phanloaibo":{"$first":"$PhanLoaiBo"}
        }},
        {"$project":{
            "soluong":1,
            "phanloaibo":1,
            # "danhsachsotaijoined":{
            #     "$reduce": {
            #             "input": "$danhsachsotai",
            #             "initialValue": "",
            #             "in": {
            #                 "$concat": [
            #                     "$$value",
            #                     {"$cond": [{"$eq": ["$$value", ""]}, "", ";"]},
            #                     "$$this",
            #                 ]
            #             },
            #         }}
        }}
    ]
    tongdan = 0
    startTime = time.time()
    results = db.bonhaptrai_aggregate(pipeline)
    for result in results:
        tongdan += result["soluong"]
        print("Soluong "+(result["phanloaibo"] if result["phanloaibo"] is not None else "")+": "+str(result["soluong"]))
    print("Tong dan: "+str(tongdan))
    endTime = time.time()
    print("Tong thoi gian: "+str(endTime-startTime))


def biendongdan(startdate,enddate):
    startDate = datetime.strptime(startdate,date_format)
    endDate = datetime.strptime(enddate,date_format)+timedelta(days=1)
    pipeline = [
        {"$match":{
            "NgaySinh":{
                "$ne":None,
                "$gte":startDate,
                "$lt":endDate
            }
        }},
        {"$group":{
            "_id":{
                "year":{"$year":"$NgaySinh"},
                "month":{"$month":"$NgaySinh"},
                "day":{"$dayOfMonth":"$NgaySinh"},
            },
            "soluong":{"$sum":1}
        }},
        {"$sort":SON([("_id.year", 1), ("_id.month", 1), ("_id.day", 1)])},
        {"$project":{
            "_id":1,
            "soluong":1
        }}
    ]
    results = db.bonhaptrai_aggregate(pipeline)
    for result in results:
        print("Ngay"+str(result["_id"]["day"])+":"+str(result["soluong"]))

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
        {"$match": {"LoCos.LoCoCus.IsQuyHach": False,"LoaiDongCo":"BanChanTha"}},
        # group lại theo nông trường
        {
            "$group": {
                "_id": "null",
                "TongDienTich": {"$sum": "$LoCos.LoCoCus.DienTich"},
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
        print("Tổng diện tích trồng các lô cỏ : "+str(nongTruong["DienTich"]))

def dientichco_theohangmuccongviec(client: MongoClient, db, hangmuccongviec,startdate,enddate,collection="NongTruongCo",):
    startDate = datetime.strptime(startdate,date_format)
    endDate = datetime.strptime(enddate,date_format) +timedelta(days=1)

    pipeline = [
        {"$match": {"HangMucCongViec.TenHangMucCongViec": {"$in":hangmuccongviec}}},
        {"$unwind":"$ThucHienCongVatTuThietBis"},
        {"$match": {"ThucHienCongVatTuThietBis.NgayThucHienChinhThuc":{"$gte":startDate,"$lt":endDate}}},
        # group lại theo nông trường
        {
            "$group": {
                "_id": "null",
                "TongDienTich": {"$sum": "$ThucHienCongVatTuThietBis.KhoiLuongQuyDoi"},
            }
        },
        # project ra document
        {
            "$project": {
                "_id": 0,
                "DienTich": "$TongDienTich",
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    dienTichTungNongTruong = list(col.aggregate(pipeline))
    for nongTruong in dienTichTungNongTruong:
        print("Tổng diện tích các lô cỏ thực hiện "+str(hangmuccongviec)+": "+str(nongTruong["DienTich"]))

def dientichco_bonphanvoco(client: MongoClient, db, hangmuccongviec,startdate,enddate,collection="NongTruongCo",):
    startDate = datetime.strptime(startdate,date_format)
    endDate = datetime.strptime(enddate,date_format) +timedelta(days=1)

    pipeline = [
        {"$match": {"HangMucCongViec.TenHangMucCongViec": {"$in":hangmuccongviec},"NgayThucHien":{"$gte":startDate},"NgayThucHien":{"$lt":endDate}}},
        # group lại theo nông trường
        {
            "$group": {
                "_id": "null",
                "TongDienTich": {"$sum": "$LoCoThucHien.DienTich"},
            }
        },
        # project ra document
        {
            "$project": {
                "_id": 0,
                "DienTich": "$TongDienTich",
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    dienTichTungNongTruong = list(col.aggregate(pipeline))
    for nongTruong in dienTichTungNongTruong:
        print("Tổng diện tích các lô cỏ thực hiện bón phân vô cơ: "+str(nongTruong["DienTich"]))

def dientichco_tegoc(client: MongoClient, db, hangmuccongviec,startdate,enddate,collection="NongTruongCo",):
    startDate = datetime.strptime(startdate,date_format)
    endDate = datetime.strptime(enddate,date_format)+timedelta(days=1)

    pipeline = [
        {"$match": {"HangMucCongViec.TenHangMucCongViec": {"$in":hangmuccongviec}}},
        {"$unwind":"$ThucHienCongVatTuThietBis"},
        {"$match": {"ThucHienCongVatTuThietBis.NgayThucHienChinhThuc":{"$gte":startDate,"$lt":endDate}}},
        # group lại theo nông trường
        {
            "$group": {
                "_id": "null",
                "TongDienTich": {"$sum": "$ThucHienCongVatTuThietBis.KhoiLuongQuyDoi"},
            }
        },
        # project ra document
        {
            "$project": {
                "_id": 0,
                "DienTich": "$TongDienTich",
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    dienTichTungNongTruong = list(col.aggregate(pipeline))
    for nongTruong in dienTichTungNongTruong:
        print("Tổng diện tích tề gốc: "+str(nongTruong["DienTich"]))

# Tổng khối lượng phân vô cơ
def tongkhoiluong_phanvoco(client: MongoClient, db, hangmucvattu,startdate,enddate,collection="NongTruongCo",):
    startDate = datetime.strptime(startdate,date_format)
    endDate = datetime.strptime(enddate,date_format)+timedelta(days=1)
    pipeline = [
        # group lại theo nông trường
        {"$unwind":"$ThucHienCongVatTuThietBis"},
        {"$match": {"ThucHienCongVatTuThietBis.NgayThucHienChinhThuc":{"$gte":startDate,"$lt":endDate}}},
        {"$unwind":"$ThucHienCongVatTuThietBis.VatTuThucHiens"},
        {"$match":{"$or":[{"ThucHienCongVatTuThietBis.VatTuThucHiens.VatTu.LoaiVatTu":"Phân vô cơ"},{"ThucHienCongVatTuThietBis.VatTuThucHiens.VatTu.TenVatTu":{"$in":hangmucvattu}}]}},
        {
            "$group": {
                "_id": "null",
                "Tongkhoiluong": {"$sum": "$ThucHienCongVatTuThietBis.VatTuThucHiens.SoLuong"},
            }
        },
        # project ra document
        {
            "$project": {
                "_id": 0,
                "KhoiLuong": "$Tongkhoiluong",
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    dienTichTungNongTruong = list(col.aggregate(pipeline))
    for nongTruong in dienTichTungNongTruong:
        print("Tổng khối lượng phân vô cơ đã dùng: "+str(nongTruong["KhoiLuong"]/1000))


def tongdientich_chantha(client: MongoClient, db, startdate,enddate,collection="HangMucCongViecChanTha",):
    startDate = datetime.strptime(startdate,date_format)
    endDate = datetime.strptime(enddate,date_format)+timedelta(days=1)
    pipeline = [
        {"$match":{
            "NgayThucHien":{
                "$gte":startDate,
                "$lt":endDate
            }
        }},
        {
            "$group": {
                "_id": "null",
                "Tongkhoiluong": {"$sum": "$LoCoCu.DienTich"},
            }
        },
        # project ra document
        {
            "$project": {
                "_id": 0,
                "KhoiLuong": "$Tongkhoiluong",
            }
        },
    ]
    dbase = client[db]
    col = dbase[collection]
    dienTichTungNongTruong = list(col.aggregate(pipeline))
    for nongTruong in dienTichTungNongTruong:
        print("Tổng diện tích chăn thả: "+str(nongTruong["KhoiLuong"]))



# Tính lượng phân đã đưa vào luống (Đã xác nhận)
def tinhTongPhanDuaVaoLuong(
    client: MongoClient, db,startdate,enddate, collection="PhieuCongViecLuongPhan"
):
    startDate = datetime.strptime(startdate,date_format)
    endDate = datetime.strptime(enddate,date_format)+timedelta(days=1)
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
