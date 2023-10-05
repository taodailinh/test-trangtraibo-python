from pymongo import MongoClient
from datetime import datetime
import time

date_format = "%Y-%m-%d"

startTime = time.time()
# Kết nối db
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
# db = client["quanlytrangtrai_0910"]

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
        date_format = "%Y-%m-%d"
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


# Tong so bo theo nhom
def thongTinDan_tongSoBo(
    client: MongoClient,
    dbName,
    collectionName,
    startdate,
    enddate,
    excelWriter,
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
                    {"NhomBo": {"$in": nhombo["danhsach"]}},
                    {"PhanLoaiBo": {"$in": nhomphanloai["danhsach"]}},
                    {"GioiTinhBe": {"$in": gioitinh["danhsach"]}},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "SoTai"},
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
        + " - "
        + (nhomphanloai["tennhom"])
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)

# Tổng số bò theo nghiệp vụ
def nghiepVu_tongSoBo(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    nhomphanloai,
    loaingay,
    field1,
    criteria,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = []
    if nhomphanloai["tennhom"]=="":
        pipeline = [
        {
            "$match": {
                "$and": [
                    {loaingay: {"$gte": startDate, "$lte": endDate}},
                    {field1:criteria}
                ]
            }
        },
        {
            "$match": {
                "$and": [
                    {"Bo.NhomBo": {"$in": nhombo["danhsach"]}},
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
    else:
        pipeline = [
        {
            "$match": {
                "$and": [
                    {loaingay: {"$gte": startDate, "$lte": endDate}},
                    {field1:criteria}
                ]
            }
        },
        {
            "$match": {
                "$and": [
                    {"Bo.NhomBo": {"$in": nhombo["danhsach"]}},
                    {"Bo.PhanLoaiBo": {"$in": nhomphanloai["danhsach"]}},
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
        + " - "
        + (nhomphanloai["tennhom"])
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 1	Tổng số bò đực giống đã được đề xuất thanh lý
def tongSoBoThanhLy_BoDucGiong(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
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
                    {"Bo.NhomBo": {"$in": nhombo["danhsach"]}},
                    {"Bo.PhanLoaiBo": {"$in": nhomphanloai["danhsach"]}},
                    {"Bo.GioiTinhBe": {"$in": gioitinh["danhsach"]}},
                    {"HinhThucThanhLy": "DeXuatThanhLy"},
                    {"NgayDeXuat": {"$gte": startDate, "$lte": endDate}},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "SoTai"},
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
        + " - "
        + (nhomphanloai["tennhom"])
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 2	Bò không đủ tiêu chuẩn xử lý sinh sản

# Tổng số bò xử lý sinh sản theo ngày
def tongSo_XLSS(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    ngayxuly,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    lieuTrinh = {"0":-1,"7":-2,"9":-3,"10":-4}
    thuTu=lieuTrinh[ngayxuly]
    pipeline = [
        {"$match":{"LieuTrinhApDungs":{"$exists":True,}}},
        {"$project":{"field1_second_item": {'$arrayElemAt': ['$LieuTrinhApDungs', thuTu]},"Bo.SoTai":1}},
        {
        "$match":  {"field1_second_item.NgayThucHien": {"$gte": startDate, "$lte": endDate}}
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
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)

# 3	Tổng số bò được xử lý hormone sinh sản ngày 0
# 4	Tổng số bò được xử lý hormone sinh sản 7
# 5	Tổng số bò được xử lý hormone sinh sản 9
# 6	Tổng số bò được xử lý hormone sinh sản 10

# 7	Tổng số bò được gieo tinh nhân tạo từ bò lên giống tự nhiên (không xử lý sinh sản)
def tongSo_phoiGiongTuNhien_ver1(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 3*24*60*60*1000
    pipeline = [
        {"$match":{"NgayPhoi":{"$gte": startDate, "$lte": endDate}}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "as":"phoigiongxuly"
        }},
        {"$match":{"phoigiongxuly.0":{"$exists":True}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":startDate,"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {
    #     # "$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}
    #     },
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

def tongSo_phoiGiongTuNhien_ver2(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 3*24*60*60*1000
    pipeline = [
        {"$match":{"NgayPhoi":{"$gte": startDate, "$lte": endDate}}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "as":"phoigiongxuly"
        }},
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
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

def tongSo_phoiGiongTuNhien_ver3(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 3*24*60*60*1000
    pipeline = [
        {"$match":{"NgayPhoi":{"$gte": startDate, "$lte": endDate}}},
        {"$sort":{"Bo.SoTai":1}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "pipeline":[{
                "$match":{"$expr":{"$gt":[{"$subtract":[startDate,{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]}]},khungChenhLech]}},
            },
            {"$sort":{"LieuTrinhApDungs.0.NgayThucHien":-1}},
            ],
            "as":"phoigiongxuly"
        }},
        {
        "$match": {"$expr":{"$gt":[{"$size":"$phoigiongxuly"},0]}}},
        {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
       {
        "$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

def tongSo_phoiGiongTuNhien(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 3*24*60*60*1000
    khungChenhLechBanDau = (3*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayPhoi":{"$gte": startDate, "$lte": endDate}}},
        {"$sort":{"Bo.SoTai":1}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"soTai":"$Bo.SoTai","ngayPhoi":"$NgayPhoi"},
            "pipeline":[
                {"$match":{"$expr":{"$lte":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},endDate]}},},
                {"$match":{"$expr":{"$gt":[{"$subtract":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},startDate]},khungChenhLechBanDau]}},},
                {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayPhoi",{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]}]},khungChenhLech]}},},
            ],
            "as":"phoigiongxuly"
        }},
        {"$match":{"$expr":{"$eq":[{"$size":"$phoigiongxuly"},0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))


#work and fast
def tongSo_phoiGiongSauXLSS(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 3*24*60*60*1000
    khungChenhLechBanDau = (3*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayPhoi":{"$gte": startDate, "$lte": endDate}}},
        {"$sort":{"Bo.SoTai":1}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"soTai":"$Bo.SoTai","ngayPhoi":"$NgayPhoi"},
            "pipeline":[
                {"$match":{"$expr":{"$lte":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},endDate]}},},
                {"$match":{"$expr":{"$gt":[{"$subtract":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},startDate]},khungChenhLechBanDau]}},},
                {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayPhoi",{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]}]},khungChenhLech]}},},
            ],
            "as":"phoigiongxuly"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiongxuly"},0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))




# 8	Tổng số bò được ghép đôi phối giống với bò đực giống
# 9	Tổng số bò gieo tinh nhân tạo được khám thai: (Chỉ tiêu đánh gia các chỉ tiêu dưới)
# 10	Tổng số bò xử lý sinh sản có thai
def tongSo_coThai_sauXLSS(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"CoThai":True}},
        {"$sort":{"Bo.SoTai":1}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"soTai":"$Bo.SoTai","ngayKham":"$NgayKham"},
            "pipeline":[
                {"$match":{"$expr":{"$lte":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},endDate]}},},
                {"$match":{"$expr":{"$gt":[{"$subtract":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},startDate]},khungChenhLechBanDau]}},},
                {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham",{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]}]},khungChenhLech]}},},
            ],
            "as":"cothaisauxuly"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$cothaisauxuly"},0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

# 11	Tổng số bò xử lý sinh sản không có thai
def tongSo_khongThai_sauXLSS(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"CoThai":False}},
        {"$sort":{"Bo.SoTai":1}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"soTai":"$Bo.SoTai","ngayKham":"$NgayKham"},
            "pipeline":[
                {"$match":{"$expr":{"$gt":[{"$subtract":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},startDate]},khungChenhLechBanDau]}},},
                {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham",{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]}]},khungChenhLech]}},},
            ],
            "as":"cothaisauxuly"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$cothaisauxuly"},0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

# 12	Tổng số bò lên giống tự nhiên được gieo tinh nhân tạo có thai
def tongSo_coThai_sauPhoi_tuNhien(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechPhoiXL = 3*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"CoThai":True}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            # {"$sort":{"Bo.SoTai":1}},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}}
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayPhoiCuoi":{"$arrayElemAt":["$phoigiong.NgayPhoi",0]}},
            "pipeline":[
                # {"$project":{"Bo.SoTai":1,"LieuTrinhApDungs":1}},
                {"$match":{"$expr":{"$gt":[{"$subtract":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},startDate]},khungChenhLechBanDau]}},},
                {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayPhoiCuoi",{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]}]},khungChenhLechPhoiXL]}},},
            ],
            "as":"xulysinhsan"
        }},
        {"$match":{"$expr":{"$eq":[{"$size":"$xulysinhsan"},0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

# 13	Tổng số bò lên giống tự nhiên được gieo tinh nhân tạo không có thai
def tongSo_khongThai_sauPhoi_tuNhien(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechPhoiXL = 3*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"CoThai":False}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            # {"$sort":{"Bo.SoTai":1}},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}}
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayPhoiCuoi":{"$arrayElemAt":["$phoigiong.NgayPhoi",0]}},
            "pipeline":[
                # {"$project":{"Bo.SoTai":1,"LieuTrinhApDungs":1}},
                {"$match":{"$expr":{"$gt":[{"$subtract":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},startDate]},khungChenhLechBanDau]}},},
                {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayPhoiCuoi",{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]}]},khungChenhLechPhoiXL]}},},
            ],
            "as":"xulysinhsan"
        }},
        {"$match":{"$expr":{"$eq":[{"$size":"$xulysinhsan"},0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

# 14	Tổng số bò ghép đực được khám thai
def tongSo_duocKhamThai_sauGhepDuc(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechPhoiXL = 3*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            # {"$sort":{"Bo.SoTai":1}},
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$lte":["$NgayGhepDuc",endDate]}},},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$lt":["$NgayGhepDuc","$$ngayKham"]}},},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"$or":[{"phoigiong.0.GhepDuc":True},{"phoigiong.0.GhepDucKhongQuaPhoi":True}]}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

# 15	Tổng số bò ghép đực có thai
def tongSo_coThai_sauGhepDuc(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechPhoiXL = 3*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"CoThai":True}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            # {"$sort":{"Bo.SoTai":1}},
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$lte":["$NgayGhepDuc",endDate]}},},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$lt":["$NgayGhepDuc","$$ngayKham"]}},},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"$or":[{"phoigiong.0.GhepDuc":True},{"phoigiong.0.GhepDucKhongQuaPhoi":True}]}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

# 16	Tổng số bò ghép đực không có thai
def tongSo_khongThai_sauGhepDuc(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechPhoiXL = 3*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"CoThai":False}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            # {"$sort":{"Bo.SoTai":1}},
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$lte":["$NgayGhepDuc",endDate]}},},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$lt":["$NgayGhepDuc","$$ngayKham"]}},},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"$or":[{"phoigiong.0.GhepDuc":True},{"phoigiong.0.GhepDucKhongQuaPhoi":True}]}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results = col.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" " + nghiepVu)
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))

#Tỷ lệ đậu thai
def tyLe_DauThai_theoLanPhoi(
    client: MongoClient,
    dbName,
    collectionName,
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    lanPhoi,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"CoThai":True}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            {"$match":{"$expr":{"$and":[{"$gte":["$LanPhoi",lanPhoi["min"]]},{"$lte":["$LanPhoi",lanPhoi["max"]]}]}}},
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc",endDate]}]}}},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc","$$ngayKham"]}]}}},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"phoigiong.0.GhepDucKhongQuaPhoi":False}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results_coThai = col.aggregate(pipeline)
    title1 = (
        "Số lượng "
        + " bò phối lần "
        +str(lanPhoi["min"])
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" có thai")
    )
    print(title1)
    soLuongCoThai = 0
    for result in results_coThai:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [title1, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
        soLuongCoThai = result["soLuong"]
    print(soLuongCoThai)
    #pipeline tính số lượng bò phối được khám thai
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            # {"$sort":{"Bo.SoTai":1}},
            {"$match":{"$expr":{"$and":[{"$gte":["$LanPhoi",lanPhoi["min"]]},{"$lte":["$LanPhoi",lanPhoi["max"]]}]}}},
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc",endDate]}]}}},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc","$$ngayKham"]}]}}},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"phoigiong.0.GhepDucKhongQuaPhoi":False}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    results_duocKhamThai = col.aggregate(pipeline)
    title2 = (
        "Số lượng "
        + "bò phối lần "
        +str(lanPhoi["min"])
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" được khám thai")
    )
    print(title2)
    soLuongKhamThai = 0
    for result in results_duocKhamThai:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [title1, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
        soLuongKhamThai = result["soLuong"]
    print(soLuongKhamThai)
    if soLuongKhamThai == 0:
        print("khong co bo duoc kham thai")
    else:
        formatted_num = "{:.1%}".format(soLuongCoThai/soLuongKhamThai)
        print("Tỷ lệ đậu thai: "+formatted_num)
        finishTime = time.time()
        print("tong thoi gian: " + str(finishTime - startTime))


# 17	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 1
# 18	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 2
# 19	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 3

def tyLe_DauThai_theoLanPhoi_theoGiongBo(
    client: MongoClient,
    dbName,
    collectionName,
    startdate,
    enddate,
    excelWriter,
    lanPhoi,
    giongBo,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"Bo.GiongBo":giongBo}},
        {"$match":{"CoThai":True}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            {"$match":{"$expr":{"$and":[{"$gte":["$LanPhoi",lanPhoi["min"]]},{"$lte":["$LanPhoi",lanPhoi["max"]]}]}}},
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc",endDate]}]}}},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc","$$ngayKham"]}]}}},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"phoigiong.0.GhepDucKhongQuaPhoi":False}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results_coThai = col.aggregate(pipeline)
    title1 = (
        "Số lượng "
        + " bò phối lần "
        +str(lanPhoi["min"])
        + " - "
        + " - "+ giongBo
        + (" - có thai")
    )
    print(title1)
    soLuongCoThai = 0
    for result in results_coThai:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [title1, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
        soLuongCoThai = result["soLuong"]
    print(soLuongCoThai)
    #pipeline tính số lượng bò phối được khám thai
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"Bo.GiongBo":giongBo}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            # {"$sort":{"Bo.SoTai":1}},
            {"$match":{"$expr":{"$and":[{"$gte":["$LanPhoi",lanPhoi["min"]]},{"$lte":["$LanPhoi",lanPhoi["max"]]}]}}},
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc",endDate]}]}}},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc","$$ngayKham"]}]}}},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"phoigiong.0.GhepDucKhongQuaPhoi":False}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    results_duocKhamThai = col.aggregate(pipeline)
    title2 = (
        "Số lượng "
        + "bò phối lần "
        +str(lanPhoi["min"])
        + " - "
        + " - "+giongBo
        + (" - được khám thai")
    )
    print(title2)
    soLuongKhamThai = 0
    for result in results_duocKhamThai:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [title1, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
        soLuongKhamThai = result["soLuong"]
    if soLuongKhamThai == 0:
        print("khong co bo duoc kham thai")
    else:
        formatted_num = "{:.1%}".format(soLuongCoThai/soLuongKhamThai)
        print("Tỷ lệ đậu thai: "+formatted_num)
        finishTime = time.time()
        print("tong thoi gian: " + str(finishTime - startTime))
    
# 20	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 1
# 21	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 2
# 22	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 3
# 23	Tỷ lệ đậu thai do ghép đực của giống bò Brahman
def tyLe_DauThai_ghepDuc_theoGiongBo(
    client: MongoClient,
    dbName,
    collectionName,
    startdate,
    enddate,
    excelWriter,
    giongBo,
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 90*24*60*60*1000
    khungChenhLechBanDau = (90*24*60*60*1000)*(-1)
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"Bo.GiongBo":giongBo}},
        {"$match":{"CoThai":True}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc",endDate]}]}}},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc","$$ngayKham"]}]}}},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"$or":[{"phoigiong.0.GhepDuc":True},{"phoigiong.0.GhepDucKhongQuaPhoi":True}]}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    startTime = time.time()
    results_coThai = col.aggregate(pipeline)
    title1 = (
        "Số lượng "
        + " bò ghép đực "
        + " - "
        + " - "+ giongBo
        + (" - có thai")
    )
    print(title1)
    soLuongCoThai = 0
    for result in results_coThai:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [title1, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
        soLuongCoThai = result["soLuong"]
    print(soLuongCoThai)
    #pipeline tính số lượng bò phối được khám thai
    pipeline = [
        {"$match":{"NgayKham":{"$gte": startDate, "$lte": endDate}}},
        {"$match":{"Bo.GiongBo":giongBo}},
        {"$lookup":{
            "from":"ThongTinPhoiGiong",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            # {"$sort":{"Bo.SoTai":1}},
            {"$match":{"$expr":{"$lte":["$NgayPhoi",endDate]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc",endDate]}]}}},
            {"$match":{"$expr":{"$gt":[{"$subtract":["$NgayPhoi",startDate]},khungChenhLechBanDau]}},},
            {"$match":{"$expr":{"$or":[{"$eq":["$NgayGhepDuc",None]},{"$lt":["$NgayGhepDuc","$$ngayKham"]}]}}},
            {"$match":{"$expr":{"$lt":[{"$subtract":["$$ngayKham","$NgayPhoi"]},khungChenhLech]}},},
            {"$sort":{"NgayPhoi":-1}},
            ],
            "as":"phoigiong"
        }},
        {"$match":{"$expr":{"$ne":[{"$size":"$phoigiong"},0]}}},
        {"$match":{"$or":[{"phoigiong.0.GhepDuc":True},{"phoigiong.0.GhepDucKhongQuaPhoi":True}]}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
    #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
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
                }
            }
        },
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    results_duocKhamThai = col.aggregate(pipeline)
    title2 = (
        "Số lượng "
        + "bò ghép đực"
        + " - "
        + " - "+giongBo
        + (" - được khám thai")
    )
    print(title2)
    soLuongKhamThai = 0
    for result in results_duocKhamThai:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [title1, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
        soLuongKhamThai = result["soLuong"]
    if soLuongKhamThai == 0:
        print("khong co bo duoc kham thai")
    else:
        formatted_num = "{:.1%}".format(soLuongCoThai/soLuongKhamThai)
        print("Tỷ lệ đậu thai: "+formatted_num)
        finishTime = time.time()
        print("tong thoi gian: " + str(finishTime - startTime))

# 24	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Drougth master lần 1
# 25	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Drougth master lần 2
# 26	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Drougth master lần 3
# 27	Tỷ lệ đậu thai do ghép đực của giống bò Drougth master
# 28	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Angus lần 1
# 29	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Angus lần 2
# 30	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Angus lần 3
# 31	Tỷ lệ đậu thai do ghép đực của giống bò Angus :
# 32	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Charolaire lần 1:
# 33	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Charolaire lần 2:
# 34	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bòCharolaire lần 3:
# 35	Tỷ lệ đậu thai do ghép đực của giống bò Charolaire :
# 36	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò BBB lần 1:
# 37	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò BBB lần 2:
# 38	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò BBB lần 3:
# 39	Tỷ lệ đậu thai do ghép đực của giống bò BBB :



# 40	Tuổi phối giống lần đầu của giống bò Brahman
# 41	Tuổi phối giống lần đầu của giống bò Drougth master
# 42	Tuổi phối giống lần đầu của giống bò Angus
# 43	Tuổi phối giống lần đầu của giống bò Charolaire
# 44	Tuổi phối giống lần đầu của giống bò BBB

# 45	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò Brahman
# 46	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò Drougth master
# 47	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò Angus
# 48	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò Charolair
# 49	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò BBB

# danh sách bò phối nhiều lần trong ngày
