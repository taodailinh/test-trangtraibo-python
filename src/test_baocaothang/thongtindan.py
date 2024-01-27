from pymongo import MongoClient
from datetime import datetime, timedelta
import time
from openpyxl import Workbook

from client import db, test_result_collection, changeFarm

date_format = "%Y-%m-%d"

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

tatCaPhanLoai = {
    "tennhom": "",
    "danhsach": [
        "BoMoiPhoi",
        "BoMangThaiNho",
        "BoChoPhoi",
        "BoXuLySinhSan",
        "BoMeNuoiConNho",
        "BoChoDe",
        "BoMangThaiLon",
        "BoMeNuoiConLon",
        "BoVoBeoNho",
        "BoHauBiChoPhoi",
        "BoNuoiThitBCT",
        "BoHauBi",
        "BoNuoiThitBCT8_12",
        "BeCaiSua",
        "BeTheoMe",
        "BeSinh",
        "BoCachLy",
        "",
    ],
}

bogiong = {
    "tennhom": "bò giống",
    "danhsach": [
        "BoMoiPhoi",
        "BoMangThaiNho",
        "BoChoPhoi",
        "BoXuLySinhSan",
        "BoMeNuoiConNho",
        "BoChoDe",
        "BoMangThaiLon",
        "BoMeNuoiConLon",
        "BoHauBiChoPhoi",
        "BoHauBi",
        "BeCaiSua",
        "BeTheoMe",
        "BeSinh",
    ],
}

bonuoithit = {
    "tennhom": "bò nuôi thịt",
    "danhsach": [
        "BoVoBeoNho",
        "BoVoBeoTrung",
        "BoVoBeoLon",
        "BoNuoiThitBCT",
        "BoNuoiThitBCT8_12",
    ],
}


# Kết nối db
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
# db = client["quanlytrangtrai_0910"]


test_result_collection.baocaothang.delete_many({"LoaiBaoCao": "ThongTinDan"})

testResultId = test_result_collection.baocaothang.insert_one(
    {"LoaiBaoCao": "ThongTinDan", "CreatedAt": datetime.now(), "KetQua": []}
).inserted_id


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
        pipline = [
            {"$match": {"NgayKham": {"$gte": startDate, "$lte": endDate}}},
            {"$group": {"_id": None, "uniketquakham": {"$addToSet": "$KetQuaKham"}}},
        ]
        ketquakhamthai = db.khamthai.aggregate(pipline)
        for ketqua in ketquakhamthai:
            print(ketqua)
        return 0
    except:
        print("Da co loi xay ra")
        return 1


def soLuongBoKham(startdate, enddate):
    try:
        startDate = datetime.strptime(startdate, date_format)
        endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
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
        soLuongBoKhamThai = db.khamthai.aggregate(pipeline)
        for bo in soLuongBoKhamThai:
            print("Số lượng bò:" + str(bo.get("total")))
        print("end")
        return 0
    except:
        return 1


def soLuongBoKhamPhoiLan1(startdate, enddate):
    try:
        startDate = datetime.strptime(startdate, date_format)
        endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
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
                    "danhsachsotai": {"$push": "$SoTai"},
                }
            },
        ]
        soLuongBoKhamThai = db.khamthai.aggregate(pipeline)
        for bo in soLuongBoKhamThai:
            print("Số lượng bò:" + str(bo.get("total")))
        return 0
    except:
        print("There is something wrong")
        return 1


# 0 Be duoi 100 ngay


def beDuoi100Ngay(danhsachnhombo):
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
    danhsachbo = db.bonhaptrai.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 1	Tổng số bò chờ phối, bo giai doan bo cho phoi va bo


def soBoChoPhoi():
    pipeline = [
        {"$match": {"NhomBo": "Bo"}},
        {
            "$match": {
                "PhanLoaiBo": {"$in": ["BoChoPhoi"]},
                "LichSuDieuTris": {
                    "$not": {
                        "$elemMatch": {
                            "TinhTrangDieuTri": {
                                "$in": ["DangDieuTri", "DaKham", "ChuaKham"]
                            }
                        }
                    }
                },
            },
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
    results = db.bonhaptrai.aggregate(pipeline)
    for result in results:
        if result != None:
            test_result = {
                "NoiDung": "#1. Tổng số bò chờ phối (Đã loại trừ bò đang điều trị)",
                "SoLuong": result["soluong"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
        print("1. Tổng số lượng bò chờ phối: " + str(result["soluong"]))


# 2	Tổng số bò mang thai từ 2-7 tháng
def soBoMangThaiNho(startdate, enddate):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {"$match": {"NhomBo": "Bo"}},
        # Trước thời điểm kiểm tra thì khám thai (có thai) hoặc phối giống
        # & Sau thời điểm kiểm tra là khám thai (có thai) hoặc đẻ
        # & lần phối gần nhất cách thời điểm kiểm tra không quá 210 ngày
        {"$project": {"SoTai": 1, "PhanLoaiBo": 1}},
        {
            "$match": {"PhanLoaiBo": "BoMangThaiNho"},
        },
        {"$group": {"_id": "null", "BoMangThaiNho": {"$count": {}}}},
        {"$project": {"_id": 0, "BoMangThaiNho": 1}},
    ]
    results = db.bonhaptrai.aggregate(pipeline)
    for result in results:
        if result != None:
            test_result = {
                "LoaiBaoCao": "ThongTinDan",
                "NoiDung": "Tổng số bò mang thai nhỏ",
                "CreatedAt": datetime.now(),
                "SoLuong": result["soluong"],
                "NgayBaoCao": datetime.now(),
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.insert_one(test_result)
        print("1. Tổng số lượng bò chờ phối: " + str(result["soluong"]))


# 3	Tổng số bò mang thai, chờ đẻ từ 8-9 tháng
def soBoMangThaiLonChoDe(danhsachnhombo, startdate, enddate):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {"$match": {"NhomBo": "Bo"}},
        {"$project": {"SoTai": 1, "PhanLoaiBo": 1}},
        {
            "$match": {"PhanLoaiBo": {"$in": ["BoMangThaiLon", "BoChoDe"]}},
        },
        {"$group": {"_id": "null", "BoMangThaiLonChoDe": {"$count": {}}}},
        {"$project": {"_id": 0, "BoMangThaiLonChoDe": 1}},
    ]
    danhsachbo = db.bonhaptrai.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 4	Tổng số bò mẹ nuôi con từ 0 - 1 tháng
def soBoNuoiConNho(danhsachnhombo, startdate, enddate):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {"$match": {"NhomBo": "Bo"}},
        {"$project": {"SoTai": 1, "PhanLoaiBo": 1}},
        {
            "$match": {"PhanLoaiBo": "BoMeNuoiConNho"},
        },
        {"$group": {"_id": "null", "BoNuoiConNho": {"$count": {}}}},
        {"$project": {"_id": 0, "BoNuoiConNho": 1}},
    ]
    danhsachbo = db.bonhaptrai.aggregate(pipeline)
    for bo in danhsachbo:
        print(bo)


# 5	Tổng số bò mẹ nuôi con từ ≥ 1 - 4 tháng
def soBoNuoiConLon(reportdate):
    reportDate = datetime.strptime(reportdate, date_format)
    firstDate = reportDate - timedelta(days=120)
    lastDate = reportDate - timedelta(days=30)
    pipeline = [
        {
            "$match": {
                "NhomBo": "Bo",
                "ThongTinSinhSans": {
                    "$elemMatch": {"NgaySinh": {"$gte": firstDate, "$lte": lastDate}}
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
    reportName = "# 5. Tổng số bò mẹ nuôi con từ ≥ 1 - 4 tháng"
    results = db.bonhaptrai.aggregate(pipeline)
    for result in results:
        if result != None:
            test_result = {
                "LoaiBaoCao": "ThongTinDan",
                "NoiDung": reportName,
                "CreatedAt": datetime.now(),
                "SoLuong": result["soluong"],
                "NgayBaoCao": datetime.now(),
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
        print(reportName + ": " + str(result["soluong"]))


# 6	Trọng lượng bình quân của bê cái cai sữa


def trongLuongBinhQuan_beCaiCaiSua():
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
                "danhsachsotai": {"$push": "$SoTai"},
                "soluong": {"$count": {}},
                "tongTrong": {"$sum": "$TrongLuongNhap"},
                "trongLuongBinhQuan": {"$avg": "$TrongLuongNhap"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
                "tongTrong": 1,
                "trongLuongBinhQuan": 1,
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
    startTime = time.time()
    reportName = "#6. Trọng lượng bình quân của bê cái cai sữa"
    results = db.bonhaptrai.aggregate(pipeline)
    for result in results:
        if result != None:
            test_result = {
                "LoaiBaoCao": "ThongTinDan",
                "NoiDung": reportName,
                "CreatedAt": datetime.now(),
                "SoLuong": result["soluong"],
                "NgayBaoCao": datetime.now(),
                "TrongLuongBinhQuan": result["trongLuongBinhQuan"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["trongLuongBinhQuan"]))
    endTime = time.time()
    print("Tổng thời gian: " + str(endTime - startTime))


# 7	Trọng lượng bình quân của bê đực cai sữa


def trongLuongBinhQuan_beDucCaiSua():
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
                "danhsachsotai": {"$push": "$SoTai"},
                "soluong": {"$count": {}},
                "tongTrong": {"$sum": "$TrongLuongNhap"},
                "trongLuongBinhQuan": {"$avg": "$TrongLuongNhap"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
                "tongTrong": 1,
                "trongLuongBinhQuan": 1,
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
    startTime = time.time()
    reportName = "# 7. Trọng lượng bình quân của bê đực cai sữa"
    results = db.bonhaptrai.aggregate(pipeline)
    for result in results:
        if result != None:
            test_result = {
                "LoaiBaoCao": "ThongTinDan",
                "NoiDung": reportName,
                "CreatedAt": datetime.now(),
                "SoLuong": result["soluong"],
                "NgayBaoCao": datetime.now(),
                "TrongLuongBinhQuan": result["trongLuongBinhQuan"],
                "DanhSachSoTai": result["danhsachsotaijoined"],
            }
            test_result_collection.baocaothang.update_one(
                {"_id": testResultId}, {"$push": {"KetQua": test_result}}
            )
            print(reportName + ": " + str(result["trongLuongBinhQuan"]))
    endTime = time.time()
    print("Tổng thời gian: " + str(endTime - startTime))


# 8	Tổng số bê cái cai sữa ≥ 4- 8 tháng
def tongSo_beCaiCaiSua():
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
    startTime = time.time()
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "8. Số lượng bê cái cai sữa"
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


# 9	Tổng số bê đực cai sữa ≥ 4- 8 tháng
def tongSo_beDucCaiSua():
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
    startTime = time.time()
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "9. Số lượng bê đực cai sữa"
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


# 10	Tổng số bê cái hậu bị 9- 12 tháng
def tongSo_beCaiHauBi():
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
                                {
                                    "$cond": [{"$eq": ["$$value", ""]}, "", ";"],
                                },
                                "$$this",
                            ]
                        },
                    }
                },
            }
        },
    ]
    results = db.bonhaptrai.aggregate(pipeline)
    startTime = time.time()
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "#10. Tổng số bê cái hậu bị 9- 12 tháng"
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


# 11	Tổng số bê đực hậu bị 9- 12 tháng
def tongSo_beDucHauBi():
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
                                {
                                    "$cond": [{"$eq": ["$$value", ""]}, "", ";"],
                                },
                                "$$this",
                            ]
                        },
                    }
                },
            }
        },
    ]
    results = db.bonhaptrai.aggregate(pipeline)
    startTime = time.time()
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "#11. Tổng số bê đực hậu bị 9- 12 tháng"
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


# 12	Tổng số bê đực nuôi thịt BCT bị 9- 12 tháng
def tongSo_beDucNuoiThit_9_12():
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
    results = db.bonhaptrai.aggregate(pipeline)
    print("12. Số lượng bê đực nuoi thit BCT")
    for result in results:
        print(result)


# 13	Tổng số bê cái nuôi thịt BCT bị 9- 12 tháng
def tongSo_beCaiNuoiThit_9_12():
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
    results = db.bonhaptrai.aggregate(pipeline)
    print("13. Số lượng bê cái nuoi thit BCT")
    for result in results:
        print(result)


# 14	Tổng số bò cái hậu bị BCT 13-18 tháng
def tongSo_boCaiHauBiChoPhoi():
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
    results = db.bonhaptrai.aggregate(pipeline)
    print("14. Số lượng bò cái hậu bị 13-18 thang")
    for result in results:
        print(result)


# 15	Tổng số bò đực hậu bị BCT 13-18 tháng
def tongSo_boDucHauBi_13_18():
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": {"$in": ["Be", "Bo", "BoDucGiong", "BoChuyenVoBeo"]}},
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
    results = db.bonhaptrai.aggregate(pipeline)
    print("15. Số lượng bò đực hậu bị 13-18 thang")
    for result in results:
        print(result)


# 16	Tổng số bò đực nuôi thịt BCT 13-18 tháng


# 17	Tổng số bò cái nuôi thịt BCT 13-18 tháng


# 18	Tổng số bò vỗ béo nhỏ
def tongSo_boVoBeoNho():
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
    results = db.bonhaptrai.aggregate(pipeline)
    print("18. Số lượng bo vo beo nho")
    for result in results:
        print(result)


# 19	Tăng trọng bình quân của BVB nhỏ
def tangTrongBinhQuan(
    startdate, enddate, phanloaibo, stt, nhombo=tatCaNhomBo["danhsach"]
):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "PhanLoaiBo": {"$in": phanloaibo["danhsach"]},
                "NhomBo": {"$in": nhombo},
            }
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$SoTai"},
            }
        },
        {"$project": {"_id": 0, "soluong": 1, "danhsachsotai": 1}},
    ]
    danhsachbo = []
    botimthays = db.bonhaptrai_aggregate(pipeline)
    for botimthay in botimthays:
        if botimthay != None:
            danhsachbo = botimthay["danhsachsotai"]

    pipeline = [
        {
            "$match": {
                "NgayCan": {"$gte": startDate, "$lt": endDate},
                "SoTai": {"$in": danhsachbo},
            }
        },
        {
            "$group": {
                "_id": "$SoTai",
                "lancan": {
                    "$push": {"NgayCan": "$NgayCan", "TrongLuong": "$TrongLuong"}
                },
            }
        },
        {
            "$project": {"SoTai": "$_id", "lancan": 1},
        },
    ]
    startTime = time.time()
    results = db.canbo_aggregate(pipeline)
    reportName = stt + ". " + "Tăng trọng bình quân của " + phanloaibo["tennhom"]
    total_results = 0
    total_difference = 0
    for result in results:
        if result != None and len(result["lancan"]) >= 2:
            total_results += 1
            min_item = min(result["lancan"], key=lambda item: item["NgayCan"])
            max_item = max(result["lancan"], key=lambda item: item["NgayCan"])
            total_difference += max_item["TrongLuong"] - min_item["TrongLuong"]
    print(reportName)
    print(str(total_results))
    print(str(total_difference))
    # test_result = {
    #     "NoiDung": reportName,
    #     "SoLuong": result["soluong"],
    #     "TongTangTrong": result["tongtangtrong"],
    #     "DanhSachSoTai": result["danhsachsotaijoined"],
    # }
    # test_result_collection.baocaothang.update_one(
    #     {"_id": testResultId}, {"$push": {"KetQua": test_result}}
    #         )
    #         print(reportName + ": " + str(result["tongtangtrong"] / result["soluong"]))
    endTime = time.time()
    print("Tổng thời gian: " + str(endTime - startTime))


# 20	Tổng số bò vỗ béo trung
def tongSo_boVoBeoTrung():
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
    results = db.bonhaptrai.aggregate(pipeline)
    print("20. Số lượng bo vo beo trung")
    for result in results:
        print(result)


# 21	Tăng trọng bình quân của BVB trung
# 22	Tổng số bò vỗ béo lớn
def tongSo_boVoBeoLon():
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
    results = db.bonhaptrai.aggregate(pipeline)
    print("22. Số lượng bo vo beo lon")
    for result in results:
        print(result)


# 23	Tăng trọng bình quân của BVB lớn
# 24	Tổng số bò sinh sản nhập trại
def tongSo_nhapTrai_boSinhSan(startdate, enddate):
    "tính tổng số bò sinh sản nhập trại trong thời kỳ"
    ngaySinhMacDinh = datetime.strptime("2022-01-01", date_format)
    ngayTuoiToiThieu = 240 * 1000 * 60 * 60 * 24
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NguonGoc": "BoNhap"},
                    {"NgayNhap": {"$gte": startDate, "$lt": endDate}},
                ]
            }
        },
        {
            "$project": {
                "SoTai": 1,
                "dayold": {
                    "$cond": [
                        {"$eq": ["$NgaySinh", None]},
                        {"$subtract": ["$NgayNhap", ngaySinhMacDinh]},
                        {"$subtract": ["$NgayNhap", "$NgaySinh"]},
                    ]
                },
            }
        },
        {"$match": {"dayold": {"$gt": ngayTuoiToiThieu}}},
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "# 24. Tổng số bò sinh sản nhập trại"
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


# 25	Tổng số bê nhập trại
def tongSo_nhapTrai_be(startdate, enddate):
    ngayTuoiToiDa = 240 * 1000 * 60 * 60 * 24
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NguonGoc": "BoNhap"},
                    {"NgaySinh": {"$ne": None}},
                    {"NgayNhap": {"$gte": startDate, "$lt": endDate}},
                ]
            }
        },
        {"$project": {"SoTai": 1, "dayold": {"$subtract": ["$NgayNhap", "$NgaySinh"]}}},
        {"$match": {"dayold": {"$lte": ngayTuoiToiDa}}},
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "25. Số lượng bê nhập trại"
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


# 26	Tổng số bê sinh ra
def tongSo_beSinh(startdate, enddate):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NguonGoc": "BeSinh"},
                    {"NgaySinh": {"$ne": None}},
                    {"NgaySinh": {"$gte": startDate, "$lt": endDate}},
                ]
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "26. Số lượng bê được sinh ra"
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


# 27	Tổng số bê chết
def tongSo_chet_be(startdate, enddate):
    ngayTuoiToiDa = 240
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgaySinh": {"$ne": None}},
                    {"NgayChet": {"$ne": None}},
                    {"NgayChet": {"$gte": startDate, "$lt": endDate}},
                ]
            }
        },
        {
            "$project": {
                "SoTai": 1,
                "daylive": {
                    "$dateDiff": {
                        "startDate": "$NgayChet",
                        "endDate": "$NgaySinh",
                        "unit": "day",
                    }
                },
            }
        },
        # {"$project": {"daylive": {"$subtract": ["$NgayChet", "$NgaySinh"]}}},
        {"$match": {"daylive": {"$lte": ngayTuoiToiDa}}},
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "27. Số lượng bê chết"
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


# 28	Tổng số bò giống xuất bán
def tongSo_bogiong_xuatban(startdate, enddate):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "XuatBan"},
                    {"PhanLoaiBo": {"$in": bogiong["danhsach"]}},
                    {"NgayXuatBan": {"$gte": startDate, "$lt": endDate}},
                ]
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "28. Số lượng bò giống xuất bán"
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


# 29	Tổng số bò vỗ béo xuất bán


def tongSo_bovobeo_xuatban(startdate, enddate):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format) + timedelta(days=1)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NhomBo": "XuatBan"},
                    {"NhomPhanLoai": {"$in": bonuoithit["danhsach"]}},
                    {"NgayXuatBan": {"$gte": startDate, "$lt": endDate}},
                ]
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "29. Số lượng bò vỗ béo xuất bán"
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


# 30	Tổng số bê bệnh đang chờ thanh lý
def tongSo_bebenh_chothanhly(reportdate):
    reportDate = datetime.strptime(reportdate, date_format)
    ngaySinhToiDa = reportDate - timedelta(days=240)
    pipeline = [
        {
            "$match": {
                "Bo.NgaySinh": {"$gte": ngaySinhToiDa},
                "DaThucHien": False,
                "HinhThucDeXuat": "DeXuatThanhLy",
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
    results = db.thanhly.aggregate(pipeline)
    reportName = "30. Số lượng bê chờ thanh lý"
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


# 31	Tổng số bò bệnh đang chờ thanh lý


def tongSo_bobenh_chothanhly(reportdate):
    reportDate = datetime.strptime(reportdate, date_format)
    ngaySinhToiThieu = reportDate - timedelta(days=240)
    pipeline = [
        {
            "$match": {
                "Bo.NgaySinh": {"$lt": ngaySinhToiThieu},
                "DaThucHien": False,
                "HinhThucDeXuat": "DeXuatThanhLy",
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
    results = db.thanhly.aggregate(pipeline)
    reportName = "31. Số lượng bò bệnh chờ thanh lý"
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


def tongSoBo(
    date,
    stt,
    nhomphanloai=tatCaPhanLoai,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    reportDate = datetime.strptime(date, date_format)
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
    reportName = (
        stt
        + ". Số lượng "
        + nhombo["tennhom"]
        + " "
        + " - "
        + (nhomphanloai["tennhom"])
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
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
    print("Tổng thời gian: " + str(endTime - startTime))


# export thong tin dan


# def exportThongTinDan():
#     startTime = time.time()
#     pipeline = [
#         {
#             "$match": {
#                 "$or": [
#                     {"NhomBo": "Be"},
#                     {"NhomBo": "Bo"},
#                 ]
#             }
#         },
#         {
#             "$project": {
#                 "SoTai": 1,
#                 "SoChip": 1,
#                 "GiongBo": 1,
#                 "NgayNhap": 1,
#                 "NgaySinh": 1,
#                 "PhanLoaiBo": 1,
#                 "TrongLuongNhap": 1,
#                 "MauDa": 1,
#                 "OChuong": 1,
#                 "LuaDe": 1,
#                 "NhomBo": 1,
#                 "GioiTinhBe": 1,
#             }
#         },
#     ]
#     print("bat dau lay du lieu")
#     results = db.bonhaptrai.aggregate(pipeline)
#     giaiDoanBo = {
#         "BoMoiPhoi": "Bò mới phối",
#         "BoMangThaiNho": "Bò mang thai nhỏ",
#         "BoChoPhoi": "Bò chờ phối",
#         "BoXuLySinhSan": "Bò xử lý sinh sản",
#         "BoMeNuoiConNho": "Bò mẹ nuôi con nhỏ",
#         "BoChoDe": "Bò chờ đẻ",
#         "BoMangThaiLon": "Bò mang thai lớn",
#         "BoMeNuoiConLon": "Bò mẹ nuôi con lớn",
#         "BoVoBeoNho": "Bò vỗ béo nhỏ",
#         "BoHauBiChoPhoi": "Bò hậu bị chờ phối",
#         "BoNuoiThitBCT": "Bò nuôi thịt BCT 13-18 tháng",
#         "BoHauBi": "Bò hậu bị",
#         "BoNuoiThitBCT8_12": "Bò nuôi thịt BCT 9-12 tháng",
#         "BeCaiSua": "Bê cai sữa",
#         "BeTheoMe": "Bê theo mẹ",
#         "BeSinh": "Bê sinh",
#         "BoCachLy": "Bò cách ly",
#         "": "",
#         None: "",
#     }
#     wb = Workbook()
#     ws = wb.active
#     print("bat dau xuat du lieu")
#     table_headings = [
#         "So tai",
#         "So chip",
#         "Giong bo",
#         "Ngay nhap",
#         "Ngay sinh",
#         "Phan loai bo",
#         "Trong luong nhap",
#         "Mau long",
#         "O chuong",
#         "Lua de",
#         "Nhom bo",
#         "Gioi tinh",
#     ]
#     for result in results:
#         row_data = [
#             result["SoTai"],
#             result["SoChip"],
#             result["GiongBo"],
#             result["NgayNhap"],
#             result["NgaySinh"],
#             giaiDoanBo[result["PhanLoaiBo"]],
#             result["TrongLuongNhap"],
#             result["MauDa"],
#             result["OChuong"],
#             result["LuaDe"],
#             result["NhomBo"],
#             result["GioiTinhBe"],
#         ]
#     print("Hoan tat ghi file")
#     fileName = datetime.now().strftime("%Y%B%d%H%M%S.xlsx")
#     wb.save(fileName)
#     print("hoan tat luu file")
#     finishTime = time.time()
#     print("tong thoi gian: " + str(finishTime - startTime))
