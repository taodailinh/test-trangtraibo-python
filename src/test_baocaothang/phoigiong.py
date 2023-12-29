import calendar
from pymongo import MongoClient
from datetime import datetime, timedelta
import time

from client import db, test_result_collection, changeFarm


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

# test_result_collection.baocaothang.delete_many({"LoaiBaoCao": "PhoiGiong"})
testResultDocument = test_result_collection.baocaothang.find_one(
    {"LoaiBaoCao": "PhoiGiong"}
)
if testResultDocument is None:
    testResultDocument = test_result_collection.baocaothang.insert_one(
        {"LoaiBaoCao": "PhoiGiong", "CreatedAt": datetime.now(), "KetQua": []}
    )
    testResultId = testResultDocument.inserted_id
else:
    testResultId = testResultDocument["_id"]
    test_result_collection.baocaothang.update_one(
        {"_id": testResultId}, {"$set": {"KetQua": [], "UpdatedAt": datetime.now()}}
    )


def printAllKetQuaKhamThai():
    try:
        ketquakhamthai = db.khamthai.distinct("KetQuaKham")
        for ketqua in ketquakhamthai:
            print(ketqua)
        return 0
    except:
        return 1


def printAllKetQuaKhamThaiInDateRange(startdate, enddate):
    try:
        startDate = datetime.strptime(startdate, date_format)
        print(startDate)
        endDate = datetime.strptime(enddate, date_format)
        print(endDate)
        pipeline = [
            {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
            {"$group": {"_id": None, "uniketquakham": {"$addToSet": "$KetQuaKham"}}},
        ]
        ketquakhamthai = db.khamthai.aggregate(pipeline)
        for ketqua in ketquakhamthai:
            print(ketqua)
        return 0
    except:
        print("Da co loi xay ra")
        return 1


def soluongBoKham(startdate, enddate):
    try:
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
        soluongBoKhamThai = db.khamthai.aggregate(pipeline)
        for bo in soluongBoKhamThai:
            print("Số lượng bò:" + str(bo.get("total")))
        print("end")
        return 0
    except:
        return 1


def soluongBoKhamPhoiLan1(startdate, enddate):
    try:
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
        soluongBoKhamThai = db.khamthai.aggregate(pipeline)
        for bo in soluongBoKhamThai:
            print("Số lượng bò:" + str(bo.get("total")))
        return 0
    except:
        print("There is something wrong")
        return 1


# Lấy danh sách bò có lần phối cuối cùng thỏa mãn điều kiện
def boPhoiLan1():
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
        {"$project": {"_id": 0, "$count": "Tongsoluong"}},
    ]
    results = db.bonhaptrai.aggregate(pipeline)
    for result in results:
        print(result)


# Tong so bo theo nhom
def thongTinDan_tongSoBo(
    startdate,
    enddate,
    nhomphanloai,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
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
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.bonhaptrai.aggregate(pipeline)
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
        print("   Số lượng:" + str(result["soluong"]))


# # Tổng số bò theo nghiệp vụ
# def nghiepVu_tongSoBo(
#     nghiepVu,
#     startdate,
#     enddate,
#     nhomphanloai,
#     loaingay,
#     field1,
#     criteria,
#     gioitinh=gioiTinhTatCa,
#     nhombo=tatCaNhomBo,
# ):
#     startDate = datetime.strptime(startdate, date_format)
#     endDate = datetime.strptime(enddate, date_format)
#     pipeline = []
#     if nhomphanloai["tennhom"]=="":
#         pipeline = [
#         {
#             "$match": {
#                 "$and": [
#                     {loaingay: {"$gte": startDate, "$lte": endDate}},
#                     {field1:criteria}
#                 ]
#             }
#         },
#         {
#             "$match": {
#                 "$and": [
#                     {"Bo.NhomBo": {"$in": nhombo["danhsach"]}},
#                     {"Bo.GioiTinhBe": {"$in": gioitinh["danhsach"]}},
#                 ]
#             }
#         },
#         {
#             "$group": {
#                 "_id": "null",
#                 "soluong": {"$count": {}},
#                 "danhsachsotai": {"$push": "$Bo.SoTai"},
#             }
#         },
#         {
#             "$project": {
#                 "_id": 0,
#                 "soluong": 1,
#                 "danhsachsotaijoined": {
#                     "$reduce": {
#                         "input": "$danhsachsotai",
#                         "initialValue": "",
#                         "in": {
#                             "$concat": [
#                                 "$$value",
#                                 {"$cond": [{"$eq": ["$$value", ""]}, "", ";"]},
#                                 "$$this",
#                             ]
#                         },
#                     }
#                 },
#             }
#         },
#     ]
#     else:
#         pipeline = [
#         {
#             "$match": {
#                 "$and": [
#                     {loaingay: {"$gte": startDate, "$lte": endDate}},
#                     {field1:criteria}
#                 ]
#             }
#         },
#         {
#             "$match": {
#                 "$and": [
#                     {"Bo.NhomBo": {"$in": nhombo["danhsach"]}},
#                     {"Bo.PhanLoaiBo": {"$in": nhomphanloai["danhsach"]}},
#                     {"Bo.GioiTinhBe": {"$in": gioitinh["danhsach"]}},
#                 ]
#             }
#         },
#         {
#             "$group": {
#                 "_id": "null",
#                 "soluong": {"$count": {}},
#                 "danhsachsotai": {"$push": "$Bo.SoTai"},
#             }
#         },
#         {
#             "$project": {
#                 "_id": 0,
#                 "soluong": 1,
#                 "danhsachsotaijoined": {
#                     "$reduce": {
#                         "input": "$danhsachsotai",
#                         "initialValue": "",
#                         "in": {
#                             "$concat": [
#                                 "$$value",
#                                 {"$cond": [{"$eq": ["$$value", ""]}, "", ";"]},
#                                 "$$this",
#                             ]
#                         },
#                     }
#                 },
#             }
#         },
#     ]
#     # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
#     # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
#     results = col.aggregate(pipeline)
#     reportName = (
#         "Số lượng "
#         + nhombo["tennhom"]
#         + " "
#         + " - "
#         + (nhomphanloai["tennhom"])
#         + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
#         + (" " + nghiepVu)
#     )
#     print(reportName)
#     for result in results:
#         print("   Số lượng:" + str(result["soluong"]))
#         row = [reportName, result["soluong"], result["danhsachsotaijoined"]]
#         excelWriter.append(row)


# 1	Tổng số bò đực giống đã được đề xuất thanh lý
def tongSoBoThanhLy_BoDucGiong(
    startdate,
    enddate,
    nhomphanloai,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"Bo.NhomBo": {"$in": nhombo["danhsach"]}},
                    {"Bo.PhanLoaiBo": {"$in": nhomphanloai["danhsach"]}},
                    {"Bo.GioiTinhBe": {"$in": gioitinh["danhsach"]}},
                    {"HinhThucThanhLy": "DeXuatThanhLy"},
                    {"NgayDeXuat": {"$gte": startDate, "$lt": endDate}},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.thanhly.aggregate(pipeline)
    reportName = "#1. Số lượng bò đực giống thanh lý"
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
        print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    print("Tổng thời gian: " + str(endTime - startTime))


# 2	Bò không đủ tiêu chuẩn xử lý sinh sản
def tongSo_boKhongDatTieuChuan(
    startdate,
    enddate,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayKham": {"$gte": startDate, "$lt": endDate}},
                    {"DatTieuChuanPhoi": False},
                ]
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.thanhly.aggregate(pipeline)
    reportName = "#2. Số lượng bò không đạt tiêu chuẩn"
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
        print(reportName + ": " + str(result["soluong"]))


# Tổng số bò xử lý sinh sản theo ngày
def tongSo_XLSS(
    startdate,
    enddate,
    ngayxuly,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    lieuTrinh = {"0": -1, "7": -2, "9": -3, "10": -4}
    thuTu = lieuTrinh[ngayxuly]
    pipeline = [
        {
            "$match": {
                "LieuTrinhApDungs": {
                    "$exists": True,
                }
            }
        },
        {
            "$project": {
                "field1_second_item": {"$arrayElemAt": ["$LieuTrinhApDungs", thuTu]},
                "Bo.SoTai": 1,
            }
        },
        {
            "$match": {
                "field1_second_item.NgayThucHien": {"$gte": startDate, "$lt": endDate},
                "field1_second_item.DaHoanThanh": True,
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.xlss.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" xử lý sinh sản ngày " + ngayxuly)
    )
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
        print(reportName + ": " + str(result["soluong"]))


# 3	Tổng số bò được xử lý hormone sinh sản ngày 0
# 4	Tổng số bò được xử lý hormone sinh sản 7
# 5	Tổng số bò được xử lý hormone sinh sản 9
# 6	Tổng số bò được xử lý hormone sinh sản 10


# 7	Tổng số bò được gieo tinh nhân tạo từ bò lên giống tự nhiên (không xử lý sinh sản)
def tongSo_phoiGiongTuNhien_ver1(
    nghiepVu,
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 3 * 24 * 60 * 60 * 1000
    pipeline = [
        {"$match": {"NgayPhoi": {"$gte": startDate, "$lte": endDate}}},
        {
            "$lookup": {
                "from": "XuLySinhSan",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "as": "phoigiongxuly",
            }
        },
        {"$match": {"phoigiongxuly.0": {"$exists": True}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":startDate,"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {
        #     # "$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}
        #     },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.phoigiong.aggregate(pipeline)
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
        print("   Số lượng:" + str(result["soluong"]))
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
    khungChenhLech = 3 * 24 * 60 * 60 * 1000
    pipeline = [
        {"$match": {"NgayPhoi": {"$gte": startDate, "$lte": endDate}}},
        {
            "$lookup": {
                "from": "XuLySinhSan",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "as": "phoigiongxuly",
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
        print("   Số lượng:" + str(result["soluong"]))
        row = [reportName, result["soluong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))


# 7 Tổng số bò được gieo tinh nhân tạo từ bò lên giống tự nhiên (không xử lý sinh sản)


def tongSo_phoiGiongTuNhien(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "NgayPhoi": {"$gte": startDate, "$lt": endDate},
                "TiemHoocmon": False,
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.phoigiong.aggregate(pipeline)
    reportName = "#7. Tổng số bò được gieo tinh nhân tạo từ bò lên giống tự nhiên (không xử lý sinh sản)"
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))


""" 
Bản bên dưới này đúng nhưng hơi khó
def tongSo_phoiGiongTuNhien(
    nghiepVu,
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)+timedelta(days=1)
    khungChenhLech = 3*24*60*60*1000 # 3 ngày
    khungChenhLechBanDau = (3*24*60*60*1000)*(-1) # -3 ngày
    pipeline = [
        {"$match":{"NgayPhoi":{"$gte": startDate, "$lte": endDate}}},
        {"$sort":{"Bo.SoTai":1}},
        {"$lookup":{
            "from":"XuLySinhSan",
            "localField":"Bo.SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"soTai":"$Bo.SoTai","ngayPhoi":"$NgayPhoi"},
            "pipeline":[
                {"$match":{"$expr":{"$lt":[{"$arrayElemAt":["$LieuTrinhApDungs.NgayThucHien",0]},endDate]}},},
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
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.phoigiong.aggregate(pipeline)
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
        print("   Số lượng:" + str(result["soluong"]))
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))
"""


# work and fast
"""
#Không dùng cái này nữa
def tongSo_phoiGiongSauXLSS(
    nghiepVu,
    startdate,
    enddate,
    excelWriter,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    khungChenhLech = 3 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (3 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayPhoi": {"$gte": startDate, "$lte": endDate}}},
        {"$sort": {"Bo.SoTai": 1}},
        {
            "$lookup": {
                "from": "XuLySinhSan",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"soTai": "$Bo.SoTai", "ngayPhoi": "$NgayPhoi"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$lte": [
                                    {
                                        "$arrayElemAt": [
                                            "$LieuTrinhApDungs.NgayThucHien",
                                            0,
                                        ]
                                    },
                                    endDate,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {
                                        "$subtract": [
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                            startDate,
                                        ]
                                    },
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {
                                        "$subtract": [
                                            "$$ngayPhoi",
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                        ]
                                    },
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                ],
                "as": "phoigiongxuly",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiongxuly"}, 0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.phoigiong.aggregate(pipeline)
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
        print("   Số lượng:" + str(result["soluong"]))
        row = [reportName, result["soluong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))
"""


# Tổng số bò phối giống sau xử lý sinh sản
def tongSo_phoiGiongXLSS(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "NgayPhoi": {"$gte": startDate, "$lt": endDate},
                "TiemHoocmon": True,
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.phoigiong.aggregate(pipeline)
    reportName = "#Tổng số bò phối giống sau xử lý sinh sản"
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
        print(reportName + ": " + str(result["soluong"]))


# 8	Tổng số bò được ghép đôi phối giống với bò đực giống
def tongSoBoGhepDuc(
    startdate,
    enddate,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "ThongTinPhoiGiongs": {
                    "$elemMatch": {
                        "NgayGhepDuc": {"$gte": startDate, "$lt": endDate},
                        "$or": [{"GhepDucKhongQuaPhoi": True}, {"GhepDuc": True}],
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "#8.Tổng số bò được ghép đôi phối giống với bò đực giống"
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


# 9	Tổng số bò gieo tinh nhân tạo được khám thai: (Chỉ tiêu đánh gia các chỉ tiêu dưới)


def tongSoBoGieoTinhNhanTaoDuocKhamThai(
    startdate,
    enddate,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    gioiHanPhoiCuoi = startDate - timedelta(days=30)
    gioiHanPhoiDau = startDate - timedelta(days=60)
    pipeline = [
        {
            "$match": {
                "ThongTinKhamThais": {
                    "$elemMatch": {
                        "NgayKham": {"$gte": startDate, "$lt": endDate},
                    }
                },
                "ThongTinPhoiGiong": {
                    "$elemMatch": {
                        "NgayKham": {"$gte": gioiHanPhoiDau, "$lt": gioiHanPhoiCuoi},
                        "GhepDucKhongQuaPhoi": False,
                    }
                },
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "#9. Tổng số bò gieo tinh nhân tạo được khám thai"
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


# 10	Tổng số bò xử lý sinh sản có thai
def tongSo_coThai_sauXLSS(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (
        -1
    )  # Khoảng cách từ ngày xlss đến ngày khám thai có thể chấp nhận
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lt": endDate}}},
        {"$match": {"CoThai": True}},
        {"$sort": {"Bo.SoTai": 1}},
        {
            "$lookup": {
                "from": "XuLySinhSan",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"soTai": "$Bo.SoTai", "ngayKham": "$NgayKham"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$lte": [
                                    {
                                        "$arrayElemAt": [
                                            "$LieuTrinhApDungs.NgayThucHien",
                                            0,
                                        ]
                                    },
                                    endDate,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {
                                        "$subtract": [
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                            startDate,
                                        ]
                                    },
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {
                                        "$subtract": [
                                            "$$ngayKham",
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                        ]
                                    },
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                ],
                "as": "cothaisauxuly",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$cothaisauxuly"}, 0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.khamthai.aggregate(pipeline)
    reportName = (
        "#10. Số lượng bò có thai sau XLSS"
        # + nhombo["tennhom"]
        # + " "
        # + " - "
        # + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


def tongSo_coThai_sauXLSS_ver2(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {
            "$match": {
                "LichSuKhamThais": {
                    "$elemMatch": {
                        "NgayKham": {"$gte": startDate, "$lt": endDate},
                        "CoThai": True,
                    }
                }
            }
        },
        # Tính ngày khám thai không thai kề trước. Nếu trước đây không có thai thì lấ
        # Xem trong đợt này
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.khamthai.aggregate(pipeline)
    reportName = (
        "Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soluong"]))
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))


# 11	Tổng số bò xử lý sinh sản không có thai
def tongSo_khongThai_sauXLSS(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lt": endDate}}},
        {"$match": {"CoThai": False}},
        {"$sort": {"Bo.SoTai": 1}},
        {
            "$lookup": {
                "from": "XuLySinhSan",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"soTai": "$Bo.SoTai", "ngayKham": "$NgayKham"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {
                                        "$subtract": [
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                            startDate,
                                        ]
                                    },
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {
                                        "$subtract": [
                                            "$$ngayKham",
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                        ]
                                    },
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                ],
                "as": "cothaisauxuly",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$cothaisauxuly"}, 0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.khamthai.aggregate(pipeline)
    reportName = (
        "#11. Tổng số bò xử lý sinh sản không có thai"
        # + nhombo["tennhom"]
        # + " "
        # + " - "
        # + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


# 12	Tổng số bò lên giống tự nhiên được gieo tinh nhân tạo có thai
def tongSo_coThai_sauPhoi_tuNhien(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechPhoiXL = 3 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
        {"$match": {"CoThai": True}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    # {"$sort":{"Bo.SoTai":1}},
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {
            "$lookup": {
                "from": "XuLySinhSan",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayPhoiCuoi": {"$arrayElemAt": ["$phoigiong.NgayPhoi", 0]}},
                "pipeline": [
                    # {"$project":{"Bo.SoTai":1,"LieuTrinhApDungs":1}},
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {
                                        "$subtract": [
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                            startDate,
                                        ]
                                    },
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {
                                        "$subtract": [
                                            "$$ngayPhoiCuoi",
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                        ]
                                    },
                                    khungChenhLechPhoiXL,
                                ]
                            }
                        },
                    },
                ],
                "as": "xulysinhsan",
            }
        },
        {"$match": {"$expr": {"$eq": [{"$size": "$xulysinhsan"}, 0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.khamthai.aggregate(pipeline)
    reportName = (
        "#12. Tổng số bò lên giống tự nhiên được gieo tinh nhân tạo có thai"
        # + nhombo["tennhom"]
        # + " "
        # + " - "
        # + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


# 13	Tổng số bò lên giống tự nhiên được gieo tinh nhân tạo không có thai
def tongSo_khongThai_sauPhoi_tuNhien(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechPhoiXL = 3 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
        {"$match": {"CoThai": False}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    # {"$sort":{"Bo.SoTai":1}},
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {
            "$lookup": {
                "from": "XuLySinhSan",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayPhoiCuoi": {"$arrayElemAt": ["$phoigiong.NgayPhoi", 0]}},
                "pipeline": [
                    # {"$project":{"Bo.SoTai":1,"LieuTrinhApDungs":1}},
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {
                                        "$subtract": [
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                            startDate,
                                        ]
                                    },
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {
                                        "$subtract": [
                                            "$$ngayPhoiCuoi",
                                            {
                                                "$arrayElemAt": [
                                                    "$LieuTrinhApDungs.NgayThucHien",
                                                    0,
                                                ]
                                            },
                                        ]
                                    },
                                    khungChenhLechPhoiXL,
                                ]
                            }
                        },
                    },
                ],
                "as": "xulysinhsan",
            }
        },
        {"$match": {"$expr": {"$eq": [{"$size": "$xulysinhsan"}, 0]}}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.khamthai.aggregate(pipeline)
    reportName = (
        "#13. Tổng số bò lên giống tự nhiên được gieo tinh nhân tạo không có thai"
        # + nhombo["tennhom"]
        # + " "
        # + " - "
        # + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


# 14	Tổng số bò ghép đực được khám thai
def tongSo_duocKhamThai_sauGhepDuc(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechPhoiXL = 3 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lt": endDate}}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    # {"$sort":{"Bo.SoTai":1}},
                    {
                        "$match": {"$expr": {"$lt": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {"$expr": {"$lt": ["$NgayGhepDuc", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {"$expr": {"$lt": ["$NgayGhepDuc", "$$ngayKham"]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {
            "$match": {
                "$or": [
                    {"phoigiong.0.GhepDuc": True},
                    {"phoigiong.0.GhepDucKhongQuaPhoi": True},
                ]
            }
        },
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.khamthai.aggregate(pipeline)
    reportName = (
        "#14. Tổng số bò ghép đực được khám thai"
        # + nhombo["tennhom"]
        # + " "
        # + " - "
        # + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


# 15	Tổng số bò ghép đực có thai
def tongSo_coThai_sauGhepDuc(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechPhoiXL = 3 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
        {"$match": {"CoThai": True}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    # {"$sort":{"Bo.SoTai":1}},
                    {
                        "$match": {"$expr": {"$lt": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {"$expr": {"$lt": ["$NgayGhepDuc", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {"$expr": {"$lt": ["$NgayGhepDuc", "$$ngayKham"]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {
            "$match": {
                "$or": [
                    {"phoigiong.0.GhepDuc": True},
                    {"phoigiong.0.GhepDucKhongQuaPhoi": True},
                ]
            }
        },
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.khamthai.aggregate(pipeline)
    reportName = (
        "#15. Tổng số bò ghép đực có thai"
        # + nhombo["tennhom"]
        # + " "
        # + " - "
        # + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


# 16	Tổng số bò ghép đực không có thai
def tongSo_khongThai_sauGhepDuc(
    startdate,
    enddate,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechPhoiXL = 3 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
        {"$match": {"CoThai": False}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    # {"$sort":{"Bo.SoTai":1}},
                    {
                        "$match": {"$expr": {"$lt": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {"$expr": {"$lt": ["$NgayGhepDuc", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {"$expr": {"$lt": ["$NgayGhepDuc", "$$ngayKham"]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {
            "$match": {
                "$or": [
                    {"phoigiong.0.GhepDuc": True},
                    {"phoigiong.0.GhepDucKhongQuaPhoi": True},
                ]
            }
        },
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.khamthai.aggregate(pipeline)
    reportName = (
        "#16. Tổng số bò ghép đực không có thai"
        # + nhombo["tennhom"]
        # + " "
        # + " - "
        # + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": reportName,
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["soluong"]))
    endTime = time.time()
    duration = endTime - startTime
    print("Finish in " + str(duration))


# Tỷ lệ đậu thai


def tyLe_DauThai_theoLanPhoi_ver1(
    startdate,
    enddate,
    lanPhoi,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lt": endDate}}},
        {"$match": {"CoThai": True}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$gte": ["$LanPhoi", lanPhoi["min"]]},
                                    {"$lte": ["$LanPhoi", lanPhoi["max"]]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {"$expr": {"$lte": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", endDate]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", "$$ngayKham"]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {"$match": {"phoigiong.0.GhepDucKhongQuaPhoi": False}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results_coThai = db.khamthai.aggregate(pipeline)
    title1 = (
        "Số lượng "
        + " bò phối lần "
        + str(lanPhoi["min"])
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" có thai")
    )
    print(title1)
    soluongCoThai = 0
    for result in results_coThai:
        print("   Số lượng:" + str(result["soluong"]))
        soluongCoThai = result["soluong"]
    print(soluongCoThai)
    # pipeline tính số lượng bò phối được khám thai
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    # {"$sort":{"Bo.SoTai":1}},
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$gte": ["$LanPhoi", lanPhoi["min"]]},
                                    {"$lte": ["$LanPhoi", lanPhoi["max"]]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {"$expr": {"$lte": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", endDate]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", "$$ngayKham"]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {"$match": {"phoigiong.0.GhepDucKhongQuaPhoi": False}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results_duocKhamThai = db.khamthai.aggregate(pipeline)
    title2 = (
        "Số lượng "
        + "bò phối lần "
        + str(lanPhoi["min"])
        + " - "
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
        + (" được khám thai")
    )
    print(title2)
    soluongKhamThai = 0
    for result in results_duocKhamThai:
        print("   Số lượng:" + str(result["soluong"]))
        soluongKhamThai = result["soluong"]
    print(soluongKhamThai)
    if soluongKhamThai == 0:
        print("khong co bo duoc kham thai")
    else:
        formatted_num = "{:.1%}".format(soluongCoThai / soluongKhamThai)
        print("Tỷ lệ đậu thai: " + formatted_num)
        finishTime = time.time()
        print("tong thoi gian: " + str(finishTime - startTime))


def tyLe_DauThai_theoLanPhoi(
    startdate,
    enddate,
    lanPhoi,
    nhombo=tatCaNhomBo,
):
    print("Lần phối" + str(lanPhoi["min"]) + "-" + str(lanPhoi["max"]))
    startDate = datetime.strptime(startdate, date_format)
    print("Ngày khám thai đầu:" + str(startDate))
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    print("Ngày khám thai cuối:" + str(endDate))
    ngayPhoiDau = startDate.replace(day=1, month=startDate.month - 2)
    print("Ngày phối đầu:" + str(ngayPhoiDau))
    ngayPhoiCuoi = startDate.replace(day=1, month=startDate.month - 1) - timedelta(
        days=1
    )
    print("Ngày phối cuối:" + str(ngayPhoiCuoi))
    pipeline = [
        {
            "$match": {
                "NhomBo": {"$in":nhombo["danhsach"]},
                "ThongTinPhoiGiongs": {
                    "$elemMatch": {
                        "LanPhoi": {"$gte": lanPhoi["min"], "$lte": lanPhoi["max"]},
                        "NgayPhoi": {"$gte": ngayPhoiDau, "$lte": ngayPhoiCuoi},
                    }
                },
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results = db.bonhaptrai.aggregate(pipeline)
    title1 = "Số lượng " + " bò phối lần " + str(lanPhoi["min"])
    soLuongDuocPhoi = 0
    danhSachSoTaiPhoi = ""
    for result in results:
        if result != None:
            soLuongDuocPhoi = result["soluong"]
            danhSachSoTaiPhoi = result["danhsachsotaijoined"]
            print(title1 + ": " + str(soLuongDuocPhoi))
    # pipeline tính số lượng bò có thai
    pipeline = [
        {
            "$match": {
                "NhomBo": {"$in":nhombo["danhsach"]},
                "ThongTinPhoiGiongs": {
                    "$elemMatch": {
                        "LanPhoi": {"$gte": lanPhoi["min"], "$lte": lanPhoi["max"]},
                        "NgayPhoi": {"$gte": ngayPhoiDau, "$lte": ngayPhoiCuoi},
                    },
                    "$not": {
                        "$elemMatch": {
                            "LanPhoi": {"$gt": lanPhoi["max"]},
                            "NgayPhoi": {"$gte": ngayPhoiDau, "$lte": ngayPhoiCuoi},
                        }
                    },
                },
                "ThongTinKhamThais": {
                    "$elemMatch": {
                        "CoThai": True,
                        "NgayKham": {"$gte": ngayPhoiDau, "$lte": endDate},
                    }
                },
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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

    if soLuongDuocPhoi == 0:
        print("Không có bò được phối")
    else:
        results = db.bonhaptrai.aggregate(pipeline)
        soLuongDauThai = 0

        title2 = (
            "Số lượng "
            + "bò phối lần "
            + str(lanPhoi["min"])
            + (" đậu thai ngay lần phối đó")
        )
        for result in results:
            if result != None:
                soLuongDauThai = result["soluong"]
                print(title2 + ": " + str(soLuongDauThai))
                tyLeDauThai = "{:.1%}".format(soLuongDauThai / soLuongDuocPhoi)
                print("Tỷ lệ đậu thai: " + str(tyLeDauThai))
                test_result = {
                    "NoiDung": "Tỷ lệ đậu thai lần phối " + str(lanPhoi["min"]),
                    "TyLeDauThai": tyLeDauThai,
                    "DanhSachSoTaiPhoi": danhSachSoTaiPhoi,
                    "DanhSachSoTaiDauThai": result["danhsachsotaijoined"],
                }
                test_result_collection.baocaothang.update_one(
                    {"_id": testResultId}, {"$push": {"KetQua": test_result}}
                )

    endTime = time.time()
    print("Tổng thoi gian: " + str(endTime - startTime))


# 17	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 1
# 18	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 2
# 19	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 3


def tyLe_DauThai_theoLanPhoi_theoGiongBo(
    startdate,
    enddate,
    lanPhoi,
    giongBo,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
        {"$match": {"Bo.GiongBo": giongBo}},
        {"$match": {"CoThai": True}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$gte": ["$LanPhoi", lanPhoi["min"]]},
                                    {"$lte": ["$LanPhoi", lanPhoi["max"]]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {"$expr": {"$lte": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", endDate]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", "$$ngayKham"]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {"$match": {"phoigiong.0.GhepDucKhongQuaPhoi": False}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results_coThai = db.khamthai.aggregate(pipeline)
    title1 = (
        "Số lượng "
        + " bò phối lần "
        + str(lanPhoi["min"])
        + " - "
        + " - "
        + giongBo
        + (" - có thai")
    )
    print(title1)
    soluongCoThai = 0
    for result in results_coThai:
        print("   Số lượng:" + str(result["soluong"]))
        soluongCoThai = result["soluong"]
    print(soluongCoThai)
    # pipeline tính số lượng bò phối được khám thai
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lt": endDate}}},
        {"$match": {"Bo.GiongBo": giongBo}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    # {"$sort":{"Bo.SoTai":1}},
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$gte": ["$LanPhoi", lanPhoi["min"]]},
                                    {"$lte": ["$LanPhoi", lanPhoi["max"]]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {"$expr": {"$lt": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", endDate]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", "$$ngayKham"]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {"$match": {"phoigiong.0.GhepDucKhongQuaPhoi": False}},
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results_duocKhamThai = db.khamthai.aggregate(pipeline)
    title2 = (
        "Số lượng "
        + "bò phối lần "
        + str(lanPhoi["min"])
        + " - "
        + " - "
        + giongBo
        + (" - được khám thai")
    )
    print(title2)
    soluongKhamThai = 0
    for result in results_duocKhamThai:
        print("   Số lượng:" + str(result["soluong"]))
        soluongKhamThai = result["soluong"]
    if soluongKhamThai == 0:
        print("khong co bo duoc kham thai")
    else:
        formatted_num = "{:.1%}".format(soluongCoThai / soluongKhamThai)
        print("Tỷ lệ đậu thai: " + formatted_num)
        finishTime = time.time()
        print("tong thoi gian: " + str(finishTime - startTime))


# 20	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 1
# 21	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 2
# 22	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 3
# 23	Tỷ lệ đậu thai do ghép đực của giống bò Brahman
def tyLe_DauThai_ghepDuc_theoGiongBo(
    startdate,
    enddate,
    giongBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    khungChenhLech = 90 * 24 * 60 * 60 * 1000
    khungChenhLechBanDau = (90 * 24 * 60 * 60 * 1000) * (-1)
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
        {"$match": {"Bo.GiongBo": giongBo}},
        {"$match": {"CoThai": True}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    {
                        "$match": {"$expr": {"$lte": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", endDate]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", "$$ngayKham"]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {
            "$match": {
                "$or": [
                    {"phoigiong.0.GhepDuc": True},
                    {"phoigiong.0.GhepDucKhongQuaPhoi": True},
                ]
            }
        },
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results_coThai = db.khamthai.aggregate(pipeline)
    title1 = "Số lượng " + " bò ghép đực " + " - " + " - " + giongBo + (" - có thai")
    print(title1)
    soluongCoThai = 0
    for result in results_coThai:
        print("   Số lượng:" + str(result["soluong"]))
        soluongCoThai = result["soluong"]
    print(soluongCoThai)
    # pipeline tính số lượng bò phối được khám thai
    pipeline = [
        {"$match": {"NgayKham": {"$gte": startDate, "$lt": endDate}}},
        {"$match": {"Bo.GiongBo": giongBo}},
        {
            "$lookup": {
                "from": "ThongTinPhoiGiong",
                "localField": "Bo.SoTai",
                "foreignField": "Bo.SoTai",
                "let": {"ngayKham": "$NgayKham"},
                "pipeline": [
                    # {"$sort":{"Bo.SoTai":1}},
                    {
                        "$match": {"$expr": {"$lt": ["$NgayPhoi", endDate]}},
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", endDate]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {"$subtract": ["$NgayPhoi", startDate]},
                                    khungChenhLechBanDau,
                                ]
                            }
                        },
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$or": [
                                    {"$eq": ["$NgayGhepDuc", None]},
                                    {"$lt": ["$NgayGhepDuc", "$$ngayKham"]},
                                ]
                            }
                        }
                    },
                    {
                        "$match": {
                            "$expr": {
                                "$lt": [
                                    {"$subtract": ["$$ngayKham", "$NgayPhoi"]},
                                    khungChenhLech,
                                ]
                            }
                        },
                    },
                    {"$sort": {"NgayPhoi": -1}},
                ],
                "as": "phoigiong",
            }
        },
        {"$match": {"$expr": {"$ne": [{"$size": "$phoigiong"}, 0]}}},
        {
            "$match": {
                "$or": [
                    {"phoigiong.0.GhepDuc": True},
                    {"phoigiong.0.GhepDucKhongQuaPhoi": True},
                ]
            }
        },
        # {"$addFields":{"ngayxlgannhat":{"$cond":{"if":{"$gt": [{ "$size": "$phoigiongxuly" }, 0]},"then":{"$arrayElemAt": [{"$arrayElemAt": ["$phoigiongxuly.LieuTrinhApDungs.NgayThucHien", 0]},0]},"else":None}}}},
        # {"$addFields":{"chenhlech":{"$cond":{"if":{"$ne": ["$ngayxlgannhat", None]},"then":{"$subtract":["$NgayPhoi","$ngayxlgannhat"]},"else":None}}}},
        #    {"$match":  {"$or":[{"chenhlech":None},{"chenhlech":{"$gt":khungChenhLech}}]}},
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
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
    results_duocKhamThai = db.khamthai.aggregate(pipeline)
    title2 = (
        "Số lượng " + "bò ghép đực" + " - " + " - " + giongBo + (" - được khám thai")
    )
    print(title2)
    soluongKhamThai = 0
    for result in results_duocKhamThai:
        print("   Số lượng:" + str(result["soluong"]))
        row = [title1, result["soluong"]]

        soluongKhamThai = result["soluong"]
    if soluongKhamThai == 0:
        print("khong co bo duoc kham thai")
    else:
        formatted_num = "{:.1%}".format(soluongCoThai / soluongKhamThai)
        print("Tỷ lệ đậu thai: " + formatted_num)
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


def tuoiPhoiGiongLanDau_theoGiongBo(
    startdate,
    enddate,
    giongbo,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {"$match": {"NgayPhoi": {"$gte": startDate, "$lt": endDate}}},
        {"$match": {"$expr": {"$ne": ["$Bo.NgaySinh", None]}}},
        {"$match": {"Bo.LanPhoi": 1}},
        {"$match": {"Bo.LuaDe": 0}},
        {
            "$lookup": {
                "from": "BoNhapTrai",
                "localField": "Bo.SoTai",
                "foreignField": "SoTai",
                "as": "phoigiong",
            }
        },
        {"$match": {"$phoigiong.0.GiongBo": giongbo}},
        {
            "$group": {
                "_id": None,
                "soluong": {"$count": {}},
                "ngaytuoitrungbinh": {"$avg": "$chenhlech"},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
                "chenhlech": 1,
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
    results = db.phoigiong.aggregate(pipeline)
    reportName = "Ngày tuổi bình quân của lần phối đầu của giống bò" + " " + giongbo
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soluong"]))
        row = [reportName, result["chenhlech"], result["danhsachsotaijoined"]]
        print(str(result["chenhlech"] / (60 * 60 * 1000)))
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))


def tuoiPhoiGiongLanDau_theoGiongBo_ver1(
    startdate,
    enddate,
    giongbo,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {"$match": {"NgayPhoi": {"$gte": startDate, "$lte": endDate}}},
        {"$match": {"$expr": {"$eq": ["$LanPhoi", 1]}}},
        {"$match": {"Bo.LuaDe": 0}},
        {
            "$lookup": {
                "from": "BoNhapTrai",
                "localField": "Bo.SoTai",
                "foreignField": "SoTai",
                "as": "phoigiong",
            }
        },
        {"$match": {"phoigiong.0.GiongBo": giongbo}},
        {"$match": {"$expr": {"$ne": ["$phoigiong.0.NgaySinh", None]}}},
        {
            "$project": {
                "Bo.SoTai": 1,
                "chenhlech": {
                    "$subtract": [
                        "$NgayPhoi",
                        {"$arrayElemAt": ["$phoigiong.NgaySinh", 0]},
                    ]
                },
            }
        },
        {
            "$group": {
                "_id": None,
                "soluong": {"$count": {}},
                "ngaytuoitrungbinh": {"$avg": "$chenhlech"},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
                "ngaytuoitrungbinh": 1,
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
    results = db.phoigiong.aggregate(pipeline)
    reportName = "Ngày tuổi bình quân của lần phối đầu của giống bò " + giongbo
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soluong"]))
        if result["ngaytuoitrungbinh"] != None:
            print(str(result["ngaytuoitrungbinh"] / (24 * 60 * 60 * 1000)))
            print(result["danhsachsotaijoined"])
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))


# 40	Tuổi phối giống lần đầu của giống bò Brahman
# 41	Tuổi phối giống lần đầu của giống bò Drougth master
# 42	Tuổi phối giống lần đầu của giống bò Angus
# 43	Tuổi phối giống lần đầu của giống bò Charolaire
# 44	Tuổi phối giống lần đầu của giống bò BBB


# Test khoảng cách giữa 2 lứa đẻ bình quân
def khoangCachGiua2LuaDe(
    startdate,
    enddate,
    giongbo,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$ne": ["$LuaDe", None]},
                        {"$gt": ["$LuaDe", 1]},
                        {"GiongBo": giongbo},
                    ]
                }
            }
        },
        # {"$match":{"$expr":{"$gt":["$LuaDe",1]}}},
        # {"$match":{"GiongBo":giongbo}},
        {"$unwind": "$ThongTinSinhSans"},
        {
            "$sort": {"ThongTinSinhSans.NgaySinh": -1},
        },
        {
            "$group": {
                "_id": "$SoTai",
                "luadecuoi": {"$first": "$ThongTinSinhSans.NgaySinh"},
                "luadedau": {"$last": "$ThongTinSinhSans.NgaySinh"},
                "LuaDe": {"$first": "$LuaDe"},
            }
        },
        {
            "$project": {
                "_id": 1,
                "chenhlech": {"$subtract": ["$luadecuoi", "$luadedau"]},
                "sokyde": {"$subtract": ["$LuaDe", 1]},
            }
        },
        {
            "$project": {
                "_id": 1,
                "khoangcachluadebinhquan": {"$divide": ["$chenhlech", "$sokyde"]},
            }
        },
        {
            "$group": {
                "_id": None,
                "soluong": {"$count": {}},
                "songaydebinhquan": {"$avg": "$khoangcachluadebinhquan"},
                "danhsachsotai": {"$push": "$_id"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
                "songaydebinhquan": 1,
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "Chênh lệch bình quân giữa 2 lứa đẻ giống bò " + giongbo
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soluong"]))
        print(
            "   Chênh lệch ngày đẻ bình quân giữa các lứa đẻ:"
            + str(result["songaydebinhquan"])
        )
    finishTime = time.time()
    print("tong thoi gian: " + str(finishTime - startTime))


# 45	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò Brahman
# 46	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò Drougth master
# 47	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò Angus
# 48	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò Charolair
# 49	Khoảng cách giữa 2 lứa đẻ bình quân của giống bò BBB

# danh sách bò phối nhiều lần trong ngày
