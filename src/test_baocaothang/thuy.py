from pymongo import MongoClient
from datetime import datetime
import time

date_format = "%Y-%m-%d"

startTime = time.time()

giaiDoanBoVoBeo = ["BoVoBeoNho", "BoVoBeoTrung", "BoVoBeoLon"]

giaiDoanBoChoPhoi = ["BoChoPhoi", "BoHauBiChoPhoi"]

tatCaNhomBo = {
    "tennhom": "bò",
    "danhsach": ["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be", None],
}

gioiTinhTatCa = {
    "tennhom": "",
    "danhsach": ["Đực", "Cái", "Không xác định", None, ""],
}


# 1,1	Tổng số bò đã điều trị khỏi bệnh
def tongSo_boKhoiBenh(client: MongoClient, dbName, collectionName, startdate, enddate):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"TinhTrangDieuTri": "KhoiBenh"},
                    {"NgayKetThucDieuTri": {"$gte": startDate, "$lte": endDate}},
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
    print("1.1 Tong so bo dieu tri khoi benh")
    for result in results:
        print(result)


# 1,2	Tổng số bò đã điều trị  (Chết):
def tongSo_boChetCoDieuTri(
    client: MongoClient, dbName, collectionName, startdate, enddate
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"TinhTrangDieuTri": "Chet"},
                    {"NgayKetThucDieuTri": {"$ne": None}},
                    {"NgayKetThucDieuTri": {"$gte": startDate, "$lte": endDate}},
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
    print("1.2 Tong so bo dieu tri chet")
    for result in results:
        print(result)


# 1,3	Tổng số bò mắc bệnh đã đề nghị bán thanh lý
def tongSo_boDaDeXuatThanhLy(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"HinhThucThanhLy": "DeXuatThanhLy"},
                    {"NgayDeXuat": {"$gte": startDate, "$lte": endDate}},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "1.3 Tong so bo da de xuat thanh ly"
    print(reportName)
    for result in results:
        print(result)
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# Tổng số bò vỗ béo đã và đang điều trị


# 2,1	Tổng số bò vỗ béo nhỏ đã và đang điều trị
def tongSo_boDaDangDieuTri_boVoBeoNho(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoVoBeoNho"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "2.1 Tong so bo vo beo nho da va dang dieu tri"
    print(reportName)
    for result in results:
        print(result)
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 2,2	Tổng số bò vỗ béo trung đã và đang điều trị
def tongSo_boDaDangDieuTri_boVoBeoTrung(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoVoBeoTrung"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "2.2 Tong so bo vo beo trung da va dang dieu tri"
    print(reportName)
    for result in results:
        print(result)
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 2,3	Tổng số bò vỗ béo lớn đã và đang điều trị
def tongSo_boDaDangDieuTri_boVoBeoLon(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoVoBeoLon"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "2.3 Tong so bo vo beo lon da va dang dieu tri"
    print(reportName)
    for result in results:
        print(result)
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 2,5	Tổng số bò vỗ béo đã điều trị Khỏi bệnh
def tongSo_boKhoiBenh_boVoBeo(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": {"$in": giaiDoanBoVoBeo}},
                    {"TinhTrangDieuTri": "KhoiBenh"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "2.5 Tổng số bò vỗ béo đã điều trị Khỏi bệnh"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 2,6	Tổng số bò vỗ béo đã điều trị không khỏi bệnh
def tongSo_boKhongKhoiBenh_boVoBeo(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": {"$in": giaiDoanBoVoBeo}},
                    {
                        "TinhTrangDieuTri": {
                            "$in": [
                                "KhongKhoiBenh",
                                "Chet",
                                "ChamSocDacBiet",
                                "ThanhLy",
                            ]
                        }
                    },
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "2.6 Tổng số bò vỗ béo đã điều trị không khỏi bệnh"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 2,7	Tổng số bò vỗ béo mắc bệnh đã đề nghị bán thanh lý
def tongSo_boDaDeXuatThanhLy_boVoBeo(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"Bo.PhanLoaiBo": {"$in": giaiDoanBoVoBeo}},
                    {"HinhThucThanhLy": "DeXuatThanhLy"},
                    {"NgayDeXuat": {"$gte": startDate, "$lte": endDate}},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "2.7 Tổng số bò vỗ béo mắc bệnh đã đề nghị bán thanh lý"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# Tổng số bò sinh sản đã và đang điều trị
# 3,1	Tổng số bò chờ phối đang điều trị
def tongSo_boDaDangDieuTri_boChoPhoi(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": {"$in": giaiDoanBoChoPhoi}},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "3.1 Tổng số bò chờ phối đang điều trị"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 3,2	Tổng số bò mang thai 2-7 tháng đã và đang điều trị
def tongSo_boDaDangDieuTri_boMangThaiNho(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoMangThaiNho"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "3.2 Tổng số bò mang thai 2-7 tháng đã và đang điều trị"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 3,3	Tổng số bò mang thai 8-9 tháng đã và đang điều trị
def tongSo_boDaDangDieuTri_boMangThaiLon(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoMangThaiLon"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "3.3 Tổng số bò mang thai 8-9 tháng đã và đang điều trị"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 3,4	Tổng số bò nuôi con 0-1 tháng đã và đang điều trị
def tongSo_boDaDangDieuTri_boNuoiConNho(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoMeNuoiConNho"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "3.4 Tổng số bò nuôi con 0-1 tháng đã và đang điều trị"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 3,5	Tổng số bò nuôi con ≥1-4 tháng đã và đang điều trị
def tongSo_boDaDangDieuTri_boNuoiConLon(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoMeNuoiConLon"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "3.5 Tổng số bò nuôi con ≥1-4 tháng đã và đang điều trị"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 3,6	Tổng số bò hậu bị  9-12 tháng đã và đang điều trị
def tongSo_boDaDangDieuTri_boHauBi(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoHauBi"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "3.6 Tổng số bò hậu bị 9-12 tháng đã và đang điều trị"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 3,7	Tổng số bò hậu bị  13-18 tháng đã và đang điều trị
def tongSo_boDaDangDieuTri_boHauBiChoPhoi(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.PhanLoaiBo": "BoHauBiChoPhoi"},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    results = col.aggregate(pipeline)
    reportName = "3.7 Tổng số bò hậu bị 13-18 tháng đã và đang điều trị"
    print(reportName)
    for result in results:
        print("   so luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 3,8	Tổng số bò thịt  13-18 tháng đã và đang điều trị
# Tổng số bê đã và đang điều trị
# 4,1	Tổng số bê từ 0-1 tháng đã và đang điều trị
# 4,2	Tổng số bê từ ≥ 1-4 tháng đã và đang điều trị
# 4,3	Tổng số bê từ cai sữa ≥ 4 tháng đến 8 tháng đã và đang điều trị
# 4,4	Tổng số bê đã điều trị khỏi bệnh
# 4,5	Tổng số bê đã điều trị không khỏi bệnh
# 4,6	Tổng số bê mắc bệnh đã đề nghị bán thanh lý
# Bệnh tật và tính thích nghi của từng giống bò


# 5,1	Tổng số bê giống Brahman từ 0-1 tháng tuổi mắc bệnh
def tongSo_boDaDangDieuTri_theoGiongBo(
    client: MongoClient,
    dbName,
    collectionName,
    startdate,
    enddate,
    excelWriter,
    giongbo,
    nhomphanloai,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKetThucDieuTri": {"$gte": startDate}},
                    {"NgayBatDauDieuTri": {"$lte": endDate}},
                    {"Bo.NhomBo": {"$in": nhombo["danhsach"]}},
                    {"Bo.PhanLoaiBo": {"$in": nhomphanloai["danhsach"]}},
                    {"Bo.GiongBo": giongbo},
                    {"Bo.GioiTinhBe": {"$in": gioitinh["danhsach"]}},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soLuong": 1,
                "danhsachsotaijoined": {
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
                    }
                },
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + giongbo
        + " - "
        + (nhomphanloai["tennhom"])
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + " - mắc bệnh"
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 5,2	Tổng số bê giống Brahman từ ≥ 1-4 tháng tuổi mắc bệnh

# 5,3	Tổng số bê cái giống Brahman từ ≥4-8 tháng tuổi mắc bệnh
# 5,4	Tổng số bê đực giống Brahman từ ≥4-8 tháng tuổi mắc bệnh
# 5,5	Tổng số bò cái giống Brahman từ 9-12 tháng tuổi mắc bệnh
# 5,6	Tổng số bò đực giống Brahman từ 9-12 tháng tuổi mắc bệnh
# 5,7	Tổng số bò cái giống Brahman từ 13-18 tháng tuổi mắc bệnh
# 5,8	Tổng số bò đực giống Brahman từ 13-18 tháng tuổi mắc bệnh
# 5,9	Tổng số bò giống Brahman chờ phối mắc bệnh
# 5,10	Tổng số bò giống Brahman mang thai 2-7 tháng mắc bệnh
# 5,11	Tổng số bò giống Brahman mang thai 8-9 tháng mắc bệnh
# 5,12	Tổng số bò giống Brahmannuôi con 0-1 tháng mắc bệnh
# 5,13	Tổng số bò giống Brahmannuôi con ≥ 2-4 tháng mắc bệnh
# 5,14	Tổng số bò đực vỗ béo nhỏ giống brahman mắc bệnh
# 5,15	Tổng số bò đực vỗ béo trung giống brahman mắc bệnh
# 5,16	Tổng số bò đực vỗ béo lớn giống brahman mắc bệnh
# 5,17	Tổng số bê giống Drougth master từ 0-1 tháng tuổi mắc bệnh
# 5,18	Tổng số bê giống Drougth master từ ≥ 1-4 tháng tuổi mắc bệnh
# 5,19	Tổng số bê cái giống Drougth master từ ≥4-8 tháng tuổi mắc bệnh
# 5,20	Tổng số bê đực giống Drougth master từ ≥4-8 tháng tuổi mắc bệnh
# 5,21	Tổng số bò cái giống Drougth master từ 9-12 tháng tuổi mắc bệnh
# 5,22	Tổng số bò đực giống Drougth master từ 9-12 tháng tuổi mắc bệnh
# 5,23	Tổng số bò cái giống Drougth master từ 13-18 tháng tuổi mắc bệnh
# 5,24	Tổng số bò đực giống Drougth master từ 13-18 tháng tuổi mắc bệnh
# 5,25	Tổng số bò giống Drougth master chờ phối mắc bệnh
# 5,26	Tổng số bò giống Drougth master mang thai 2-7 tháng mắc bệnh
# 5,27	Tổng số bò giống Drougth master mang thai 8-9 tháng mắc bệnh
# 5,28	Tổng số bò giống Drougth master nuôi con 0-1 tháng mắc bệnh
# 5,29	Tổng số bò giống Drougth master nuôi con ≥ 2-4 tháng mắc bệnh
# 5,30	Tổng số bò đực vỗ béo nhỏ giống Drougth master mắc bệnh
# 5,31	Tổng số bò đực vỗ béo trung giống Drougth master mắc bệnh
# 5,32	Tổng số bò đực vỗ béo lớn giống Drougth master mắc bệnh
# 5,33	Tổng số bê giống Angus từ 0-1 tháng tuổi mắc bệnh
# 5,34	Tổng số bê giống Angus từ ≥ 1-4 tháng tuổi mắc bệnh
# 5,35	Tổng số bê cái giống Angus từ ≥4-8 tháng tuổi mắc bệnh
# 5,36	Tổng số bê đực giống Angus từ ≥4-8 tháng tuổi mắc bệnh
# 5,37	Tổng số bò cái giống Angus từ 9-12 tháng tuổi mắc bệnh
# 5,38	Tổng số bò đực giống Angus từ 9-12 tháng tuổi mắc bệnh
# 5,39	Tổng số bò cái giống Angus từ 13-18 tháng tuổi mắc bệnh
# 5,40	Tổng số bò đực giống Angus từ 13-18 tháng tuổi mắc bệnh
# 5,41	Tổng số bò giống Angus chờ phối mắc bệnh
# 5,42	Tổng số bò giống Angus mang thai 2-7 tháng mắc bệnh
# 5,43	Tổng số bò giống Angus mang thai 8-9 tháng mắc bệnh
# 5,44	Tổng số bò giống Angus nuôi con 0-1 tháng mắc bệnh
# 5,45	Tổng số bò giống Angus nuôi con ≥ 2-4 tháng mắc bệnh
# 5,46	Tổng số bò đực vỗ béo nhỏ giống Angus mắc bệnh
# 5,47	Tổng số bò đực vỗ béo trung giống Angus mắc bệnh
# 5,48	Tổng số bò đực vỗ béo lớn giống Angus mắc bệnh
# 5,49	Tổng số bê giống Charolaire từ 0-1 tháng tuổi mắc bệnh
# 5,50	Tổng số bê giống Charolaire từ ≥ 1-4 tháng tuổi mắc bệnh
# 5,51	Tổng số bê cái giống Charolaire từ ≥4-8 tháng tuổi mắc bệnh
# 5,52	Tổng số bê đực giống Charolaire từ ≥4-8 tháng tuổi mắc bệnh
# 5,53	Tổng số bò cái giống Charolaire từ 9-12 tháng tuổi mắc bệnh
# 5,54	Tổng số bò đực giống Charolaire từ 9-12 tháng tuổi mắc bệnh
# 5,55	Tổng số bò cái giống Charolaire từ 13-18 tháng tuổi mắc bệnh
# 5,56	Tổng số bò đực giống Charolaire từ 13-18 tháng tuổi mắc bệnh
# 5,57	Tổng số bò giốngCharolaire chờ phối mắc bệnh
# 5,58	Tổng số bò giống Charolaire mang thai 2-7 tháng mắc bệnh
# 5,59	Tổng số bò giống Charolaire mang thai 8-9 tháng mắc bệnh
# 5,60	Tổng số bò giống Charolaire nuôi con 0-1 tháng mắc bệnh
# 5,61	Tổng số bò giống Charolaire nuôi con ≥ 2-4 tháng mắc bệnh
# 5,62	Tổng số bò đực vỗ béo nhỏ giống Charolaire mắc bệnh
# 5,63	Tổng số bò đực vỗ béo trung giống Charolaire mắc bệnh
# 5,64	Tổng số bò đực vỗ béo lớn giống Charolaire mắc bệnh
# 5,65	Tổng số bê giống BBB (Blan Blue Belgium) từ 0-1 tháng tuổi mắc bệnh
# 5,66	Tổng số bê giống BBB (Blan Blue Belgium) từ ≥ 1-4 tháng tuổi mắc bệnh
# 5,67	Tổng số bê cái giống BBB (Blan Blue Belgium) từ ≥4-8 tháng tuổi mắc bệnh
# 5,68	Tổng số bê đực giống BBB (Blan Blue Belgium) từ ≥4-8 tháng tuổi mắc bệnh
# 5,69	Tổng số bò cái giống BBB (Blan Blue Belgium) từ 9-12 tháng tuổi mắc bệnh
# 5,70	Tổng số bò đực giống BBB (Blan Blue Belgium) từ 9-12 tháng tuổi mắc bệnh
# 5,71	Tổng số bò cái giống BBB (Blan Blue Belgium) từ 13-18 tháng tuổi mắc bệnh
# 5,72	Tổng số bò đực giống BBB (Blan Blue Belgium) từ 13-18 tháng tuổi mắc bệnh
# 5,73	Tổng số bò giống BBB (Blan Blue Belgium) chờ phối mắc bệnh
# 5,74	Tổng số bò giống BBB (Blan Blue Belgium) mang thai 2-7 tháng mắc bệnh
# 5,75	Tổng số bò giống BBB (Blan Blue Belgium) mang thai 8-9 tháng mắc bệnh
# 5,76	Tổng số bò giống BBB (Blan Blue Belgium) nuôi con 0-1 tháng mắc bệnh
# 5,77	Tổng số bò giống BBB (Blan Blue Belgium) nuôi con ≥ 2-4 tháng mắc bệnh
# 5,78	Tổng số bò đực vỗ béo nhỏ giống BBB (Blan Blue Belgium) mắc bệnh
# 5,79	Tổng số bò đực vỗ béo trung giống BBB (Blan Blue Belgium) mắc bệnh
# 5,80	Tổng số bò đực vỗ béo lớn giống BBB (Blan Blue Belgium) mắc bệnh
