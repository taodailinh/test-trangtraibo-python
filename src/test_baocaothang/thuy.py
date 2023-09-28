from pymongo import MongoClient
from datetime import datetime
import time

date_format = "%Y-%m-%d"

startTime = time.time()
# Kết nối db
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
# db = client["quanlytrangtrai_0910"]


def printAllKetQuaKhamThai(client: MongoClient, databaseName, collectionName):
    try:
        db = client[databaseName]
        khamThai = db[collectionName]
        ketquakhamthai = khamThai.distinct("KetQuaKham")
        for ketqua in ketquakhamthai:
            print(ketqua)
        return 0
    except:
        return 1


def printAllKetQuaKhamThaiInDateRange(
    client: MongoClient, databaseName, collectionName, startdate, enddate
):
    try:
        startDate = datetime.strptime(startdate, date_format)
        print(startDate)
        endDate = datetime.strptime(enddate, date_format)
        print(endDate)
        db = client[databaseName]
        khamThai = db[collectionName]
        pipline = [
            {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
            {"$group": {"_id": None, "uniketquakham": {"$addToSet": "$KetQuaKham"}}},
        ]
        ketquakhamthai = khamThai.aggregate(pipline)
        for ketqua in ketquakhamthai:
            print(ketqua)
        return 0
    except:
        print("Da co loi xay ra")
        return 1


def soLuongBoKham(
    client: MongoClient, databaseName, collectionName, startdate, enddate
):
    try:
        db = client[databaseName]
        khamThai = db[collectionName]
        startDate = datetime.strptime(startdate, date_format)
        endDate = datetime.strptime(enddate, date_format)
        pipeline = [
            {
                "$match": {
                    "NgayKham": {"$gte": startDate, "$lte": endDate},
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
        print("end")
        return 0
    except:
        return 1


def soLuongBoKhamPhoiLan1(
    client: MongoClient, databaseName, collectionName, startdate, enddate
):
    try:
        db = client[databaseName]
        khamThai = db[collectionName]
        startDate = datetime.strptime(startdate, date_format)
        endDate = datetime.strptime(enddate, date_format)
        pipeline = [
            {
                "$lookup": {
                    "from": "BoNhapTrai",
                    "localField": "Bo.SoTai",
                    "foreignField": "SoTai",
                    "as": "bo",
                }
            },
            {
                "$match": {
                    "NgayKham": {"$gte": startDate, "$lte": endDate},
                    "KetQuaKham": "Có thai",
                    "$expr": {
                        "$eq": [
                            {
                                "$arrayElemAt": [
                                    "$bo.ThongTinPhoiGiongs.LanPhoi",
                                    -1,
                                ],
                            },
                            1,
                        ],
                    },
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
        return 0
    except:
        print("There is something wrong")
        return 1


# Lấy danh sách bò có lần phối cuối cùng thỏa mãn điều kiện
def boPhoiLan1(client: MongoClient, databaseName, collectionName):
    db = client[databaseName]
    danBo = db[collectionName]
    startDate = datetime(2023, 9, 1)
    endDate = datetime(2023, 9, 18)
    pipeline = [
        # Giới hạn ngày giờ
        {
            "$match": {
                "$NgayThucHien": {
                    "$gte": startDate,
                    "$lte": endDate,
                },
                "$LanPhoi": 1,
            }
        },
        # group lại
        {
            "$group": {
                "_id": "$SoTai",
            }
        },
        # project ra document
        {"$project": {"_id": 0, "$count": "TongSoLuong"}},
    ]
    results = danBo.aggregate(pipeline)
    for result in results:
        print(result)


# 0 Be duoi 100 ngay


def beDuoi100Ngay(client: MongoClient, dbName, collectionName, danhsachnhombo):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {"$match": {"NhomBo": "Be"}},
        {
            "$project": {
                "SoTai": 1,
                "NgayTuoi": {
                    "$floor": {
                        "$divide": [
                            {
                                "$subtract": [
                                    "$$NOW",
                                    "$NgaySinh",
                                ]
                            },
                            1000 * 60 * 60 * 24,
                        ]
                    }
                },
            }
        },
        {"$limit": 100},
    ]
    danhsachbo = col.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 1	Tổng số bò chờ phối, bo giai doan bo cho phoi va bo


def soBoChoPhoi(
    client: MongoClient, dbName, collectionName, danhsachnhombo, startdate, enddate
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {"$match": {"$and": [{"NhomBo": "Bo"}, {"TenantId": "0"}]}},
        {"$project": {"SoTai": 1, "PhanLoaiBo": 1}},
        {
            "$match": {"PhanLoaiBo": {"$in": ["BoChoPhoi", "BoHauBiChoPhoi"]}},
        },
        {"$group": {"_id": "null", "TongSoChoPhoi": {"$count": {}}}},
        {"$project": {"_id": 0, "TongSoChoPhoi": 1}},
    ]
    danhsachbo = col.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 2	Tổng số bò mang thai từ 2-7 tháng
def soBoMangThaiNho(
    client: MongoClient, dbName, collectionName, danhsachnhombo, startdate, enddate
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {"$match": {"NhomBo": "Bo"}},
        {"$project": {"SoTai": 1, "PhanLoaiBo": 1}},
        {
            "$match": {"PhanLoaiBo": "BoMangThaiNho"},
        },
        {"$group": {"_id": "null", "BoMangThaiNho": {"$count": {}}}},
        {"$project": {"_id": 0, "BoMangThaiNho": 1}},
    ]
    danhsachbo = col.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 3	Tổng số bò mang thai, chờ đẻ từ 8-9 tháng
def soBoMangThaiLonChoDe(
    client: MongoClient, dbName, collectionName, danhsachnhombo, startdate, enddate
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {"$match": {"NhomBo": "Bo"}},
        {"$project": {"SoTai": 1, "PhanLoaiBo": 1}},
        {
            "$match": {"PhanLoaiBo": {"$in": ["BoMangThaiLon", "BoChoDe"]}},
        },
        {"$group": {"_id": "null", "BoMangThaiLonChoDe": {"$count": {}}}},
        {"$project": {"_id": 0, "BoMangThaiLonChoDe": 1}},
    ]
    danhsachbo = col.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 4	Tổng số bò mẹ nuôi con từ 0 - 1 tháng
def soBoNuoiConNho(
    client: MongoClient, dbName, collectionName, danhsachnhombo, startdate, enddate
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {"$match": {"NhomBo": "Bo"}},
        {"$project": {"SoTai": 1, "PhanLoaiBo": 1}},
        {
            "$match": {"PhanLoaiBo": "BoMeNuoiConNho"},
        },
        {"$group": {"_id": "null", "BoNuoiConNho": {"$count": {}}}},
        {"$project": {"_id": 0, "BoNuoiConNho": 1}},
    ]
    danhsachbo = col.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 5	Tổng số bò mẹ nuôi con từ ≥ 1 - 4 tháng
def soBoNuoiConLon(
    client: MongoClient, dbName, collectionName, danhsachnhombo, startdate, enddate
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {"$match": {"NhomBo": "Bo"}},
        {"$project": {"SoTai": 1, "PhanLoaiBo": 1}},
        {
            "$match": {"PhanLoaiBo": "BoMeNuoiConLon"},
        },
        {"$group": {"_id": "null", "BoMeNuoiConLon": {"$count": {}}}},
        {"$project": {"_id": 0, "BoMeNuoiConLon": 1}},
    ]
    danhsachbo = col.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 6	Trọng lượng bình quân của bê cái cai sữa


def trongLuongBinhQuan_beCaiCaiSua(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "Be"},
                    {"PhanLoaiBo": "BeCaiSua"},
                    {"GioiTinhBe": "Cái"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "tongTrong": {"$sum": "$TrongLuongNhap"},
                "trongLuongBinhQuan": {"$avg": "$TrongLuongNhap"},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1, "tongTrong": 1, "trongLuongBinhQuan": 1}},
    ]
    results = col.aggregate(pipeline)
    print("6. Trong luong binh quan be cai cai sua")
    for result in results:
        print(result)


# 7	Trọng lượng bình quân của bê đực cai sữa


def trongLuongBinhQuan_beDucCaiSua(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "Be"},
                    {"PhanLoaiBo": "BeCaiSua"},
                    {"GioiTinhBe": "Đực"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "tongTrong": {"$sum": "$TrongLuongNhap"},
                "trongLuongBinhQuan": {"$avg": "$TrongLuongNhap"},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1, "tongTrong": 1, "trongLuongBinhQuan": 1}},
    ]
    results = col.aggregate(pipeline)
    print("7. Trong luong binh quan be đực cai sua")
    for result in results:
        print(result)


# 8	Tổng số bê cái cai sữa ≥ 4- 8 tháng
def tongSo_beCaiCaiSua(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "Be"},
                    {"PhanLoaiBo": "BeCaiSua"},
                    {"GioiTinhBe": "Cái"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("8. Số lượng bê cái cai sữa")
    for result in results:
        print(result)


# 9	Tổng số bê đực cai sữa ≥ 4- 8 tháng
def tongSo_beDucCaiSua(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "Be"},
                    {"PhanLoaiBo": "BeCaiSua"},
                    {"GioiTinhBe": "Đực"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("9. Số lượng bê đực cai sữa")
    for result in results:
        print(result)


# 10	Tổng số bê cái hậu bị 9- 12 tháng
def tongSo_beCaiHauBi(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": {"$in": ["Be", "Bo"]}},
                    {"PhanLoaiBo": "BoHauBi"},
                    {"GioiTinhBe": "Cái"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("10. Số lượng bê cái hậu bị")
    for result in results:
        print(result)


# 11	Tổng số bê đực hậu bị 9- 12 tháng
def tongSo_beDucHauBi(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": {"$in": ["Be", "Bo"]}},
                    {"PhanLoaiBo": "BoHauBi"},
                    {"GioiTinhBe": "Đực"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("10. Số lượng bê đực hậu bị")
    for result in results:
        print(result)


# 12	Tổng số bê đực nuôi thịt BCT bị 9- 12 tháng
def tongSo_beDucNuoiThit_9_12(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "BoChuyenVoBeo"},
                    {"PhanLoaiBo": "BoNuoiThitBCT"},
                    {"GioiTinhBe": "Đực"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("12. Số lượng bê đực nuoi thit BCT")
    for result in results:
        print(result)


# 13	Tổng số bê cái nuôi thịt BCT bị 9- 12 tháng
def tongSo_beCaiNuoiThit_9_12(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "BoChuyenVoBeo"},
                    {"PhanLoaiBo": "BoNuoiThitBCT"},
                    {"GioiTinhBe": "Cái"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("13. Số lượng bê cái nuoi thit BCT")
    for result in results:
        print(result)


# 14	Tổng số bò cái hậu bị BCT 13-18 tháng
def tongSo_boCaiHauBiChoPhoi(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": {"$in": ["Be", "Bo"]}},
                    {"PhanLoaiBo": "BoHauBiChoPhoi"},
                    {"GioiTinhBe": "Cái"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("14. Số lượng bò cái hậu bị 13-18 thang")
    for result in results:
        print(result)


# 15	Tổng số bò đực hậu bị BCT 13-18 tháng
def tongSo_boDucHauBi_13_18(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": {"$in": ["Be", "Bo"]}},
                    {"PhanLoaiBo": "BoHauBiChoPhoi"},
                    {"GioiTinhBe": "Đực"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("15. Số lượng bò đực hậu bị 13-18 thang")
    for result in results:
        print(result)


# 16	Tổng số bò đực nuôi thịt BCT 13-18 tháng


# 17	Tổng số bò cái nuôi thịt BCT 13-18 tháng


# 18	Tổng số bò vỗ béo nhỏ
def tongSo_boVoBeoNho(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "BoChuyenVoBeo"},
                    {"PhanLoaiBo": "BoVoBeoNho"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("18. Số lượng bo vo beo nho")
    for result in results:
        print(result)


"""
# 19	Tăng trọng bình quân của BVB nhỏ
def tangTrongBinhQuan_boVoBeoNho(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "BoChuyenVoBeo"},
                    {"PhanLoaiBo": "BoVoBeoNho"},
                ]
            }
        },
        {"$lookup":{
            "from":"CanBo",
            "localField":"SoTai",
            "foreignField":"SoTai",
            "as":"lichsucan"
        }},
        {"$unwind":"$lichsucan"},

        {
            "$group": {
                "_id": "SoTai",
                "ngayCanDau":
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("18. Số lượng bo vo beo nho")
    for result in results:
        print(result)
"""
# 20	Tổng số bò vỗ béo trung
def tongSo_boVoBeoTrung(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "BoChuyenVoBeo"},
                    {"PhanLoaiBo": "BoVoBeoTrung"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("20. Số lượng bo vo beo trung")
    for result in results:
        print(result)

# 21	Tăng trọng bình quân của BVB trung
# 22	Tổng số bò vỗ béo lớn
def tongSo_boVoBeoLon(client: MongoClient, dbName, collectionName):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "BoChuyenVoBeo"},
                    {"PhanLoaiBo": "BoVoBeoLon"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
            }
        },
        {"$project": {"_id": 0, "soLuong": 1}},
    ]
    results = col.aggregate(pipeline)
    print("22. Số lượng bo vo beo lon")
    for result in results:
        print(result)

# 23	Tăng trọng bình quân của BVB lớn
# 24	Tổng số bò sinh sản nhập trại
# 25	Tổng số bê nhập trại
# 26	Tổng số bê sinh ra
# 27	Tổng số bê chết
# 28	Tổng số bò giống xuất bán
# 29	Tổng số bò vỗ béo xuất bán
# 30	Tổng số bê bệnh đang chờ thanh lý
# 31	Tổng số bò bệnh đang chờ thanh lý


# Print danh sach dan
def danhsachdan(client: MongoClient, dbName, collectionName, pageNumber, pageSize):
    db = client[dbName]
    col = db[collectionName]
    pipeline = [
        {
            "$match": {
                "$or": [
                    {"NhomBo": "Be"},
                    {"NhomBo": "Bo"},
                ]
            }
        },
        {"$skip": (pageNumber - 1) * pageSize},
        {"$limit": pageSize},
        {
            "$project": {
                "SoTai": 1,
                "SoChip": 1,
                "GiongBo": 1,
                "NgayNhap": 1,
                "NgaySinh": 1,
                "PhanLoaiBo": 1,
                "TrongLuongNhap": 1,
                "MauLong": 1,
            }
        },
    ]
    results = col.aggregate(pipeline)
    print("10. Số lượng bê đực hậu bị")
    for result in results:
        print(result)
