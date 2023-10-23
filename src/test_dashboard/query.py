from pymongo import MongoClient
from datetime import datetime, timedelta
import time

date_format = "%Y-%m-%d"

def tongdanbo(client: MongoClient,databaseName, collectionName,nhombo,phanloaibo, worksheet):
    db = client[databaseName]
    collection = db[collectionName]
    pipeline = [
        {"$match":{"$and":[{"NhomBo":{"$in":nhombo["danhsach"]}},{"PhanLoaiBo":{"$in":phanloaibo["danhsach"]}}]}},
        {"$group":{
            "_id":None,
            "soluong":{"$count":{}},
            "danhsachsotai":{"$push":"$SoTai"}
        }},
        {"$project":{
            "soluong":1,
            "danhsachsotaijoined":{
                "$reduce": {
                        "input": "$danhsachsotai",
                        "initialValue": "",
                        "in": {
                            "$concat": [
                                "$$value",
                                {"$cond": [{"$eq": ["$$value", ""]}, "", ";"]},
                                "$$this",
                            ]
                        },
                    }}
        }}
    ]
    results = collection.aggregate(pipeline)
    title = "Số lượng "+nhombo["tennhom"]+phanloaibo["tennhom"]+": "
    for result in results:
        print(title+str(result["soluong"]))
        worksheet.append([title,result["soluong"],result["danhsachsotaijoined"]])




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
        {"$match": {"HangMucCongViec.TenHangMucCongViec": hangmuccongviec,"ThucHienCongVatTuThietBis":{"$elemMatch":{"NgayThucHien":{"$gte":startDate,"$lt":endDate}}}}},
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
        print("Tổng diện tích các lô cỏ thực hiện "+hangmuccongviec+": "+str(nongTruong["DienTich"]))

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
        {"$match":{"ThucHienCongVatTuThietBis.VatTuThucHiens.VatTu.LoaiVatTu":"Phân vô cơ"}},
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
                "Tongkhoiluong": {"$sum": "$LoCo.DienTich"},
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
