from pymongo import MongoClient
from datetime import datetime
import time
from openpyxl import Workbook
import sys
from client import db, test_result_collection

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

tatCaPhanLoai = {"tennhom":"","danhsach":["BoMoiPhoi",
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
]}

# Kết nối db
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
# db = client["quanlytrangtrai_0910"]
testResultId = None
duLieuChuongTrai = test_result_collection.query.find_one(
    {"LoaiDuLieu": "ChuongTrai"}
)
if duLieuChuongTrai is None:
    duLieuChuongTrai = test_result_collection.query.insert_one(
        {"LoaiDuLieu": "ChuongTrai", "CreatedAt": datetime.now(), "KetQua": []}
    )
    testResultId = duLieuChuongTrai.inserted_id
else:
    testResultId = duLieuChuongTrai["_id"]
    test_result_collection.query.update_one(
        {"_id": testResultId}, {"$set": {"KetQua": [], "UpdatedAt": datetime.now()}}
    )

# Tổng số bò sai lứa đẻ
def tongSoBo_saiLuaDe(
    client: MongoClient,
    dbName,
    collectionName,
    startdate,
    enddate,
    excelWriter,
    nhomphanloai=tatCaPhanLoai,
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
        {"$match":{"$expr":{"$ne":[{"$size":"$ThongTinSinhSans"},"$LuaDe"]}}},
        {
            "$group": {
                "_id": "null",
                "soLuong": {"$count": {}},
                "danhsachsotai": {"$push": "$SoTai"},
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
        + " - "
        + ((" " + nhomphanloai["tennhom"]) if nhomphanloai["tennhom"] else "")
        + ((" " + gioitinh["tennhom"]) if gioitinh["tennhom"] else "")
    )
    print(reportName)
    for result in results:
        print("   Số lượng:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


def danhsachbe(client: MongoClient,
    dbName,
    collectionName,
    startdate,
    enddate,
    excelWriter,
    nhomphanloai=tatCaPhanLoai,
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
                    {"NhomBo": {"$in": ["Bo","Be"]}},
                ]
            }
        },
        {
            "$project": {
                "SoTai":1,
                "GiongBo":1,
                "MauDa":1,
                "TheHe":1,
                "PhanLoaiBo":1,
                "TrongLuongNhap":1,
                "NhomBo":1
        },
        }
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    start = time.time()
    results = list(col.aggregate(pipeline))
    number =len(results)
    print(str(number))
    print(sys.getsizeof(results))
    end = time.time()
    totaltime = end-start
    print(str(totaltime))
    for result in results:
        excelWriter.append([result["SoTai"],result["GiongBo"],result["PhanLoaiBo"],result["TrongLuongNhap"],result["TheHe"]])


def tinh_tongsobobe(client: MongoClient,
    dbName,
    collectionName,
    startdate,
    enddate,
    excelWriter,
    nhomphanloai=tatCaPhanLoai,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    db = client[dbName]
    col = db[collectionName]
    # startDate = datetime.strptime(startdate, date_format)
    # endDate = datetime.strptime(enddate, date_format)
    # pipeline = [
    #     {
    #         "$match": {
    #             "$and": [
    #                 {"NhomBo": {"$in": nhombo["danhsach"]}},
    #             ]
    #         }
    #     },
    #     {
    #         "$project": {
    #             "SoTai":1,
    #             "GiongBo":1,
    #             "MauDa":1,
    #             "TheHe":1,
    #             "PhanLoaiBo":1,
    #             "TrongLuongNhap":1,
    #             "NhomBo":1
    #     },
    #     }
    # ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    start = time.time()
    results = col.count_documents({"NhomBo":{"$in":["Bo","Be"]}})
    # number =len(results)
    print(str(results))
    print(sys.getsizeof(results))
    end = time.time()
    totaltime = end-start
    print(str(totaltime))


def lichsuchuyenchuong(client: MongoClient,
    dbName,
    collectionName,
    sotai,
    nhomphanloai=tatCaPhanLoai,
    gioitinh=gioiTinhTatCa,
    nhombo=tatCaNhomBo,
):
    print("Lịch sử chuyển chuồng số tai: "+ sotai)
    db = client[dbName]
    col = db[collectionName]
    # startDate = datetime.strptime(startdate, date_format)
    # endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "DanhSachChuyens":{
                    "$elemMatch":{
                        "SoTai":sotai,
                    }
                }
            }
        },
        {
            "$unwind":"$DanhSachChuyens",
        },
        {"$match":{
            "DanhSachChuyens.SoTai":sotai,
        }},
        {
            "$project": {
                "LoaiDieuChuyen":1,
                "NghiepVu":1,
                "DanhSachChuyens.SoTai":1,
                "DanhSachChuyens.CreatedAt":1,
                "DanhSachChuyens.OChuongHienTai":1,
                "DanhSachChuyens.OchuongChuanBiChuyen":1,
        },
        }
    ]
    # gioiTinhRaw = ["" if x is None else x for x in gioitinh["tennhom"]]
    # gioiTinhLoaiNullJoined = " & ".join([x for x in gioiTinhRaw if x])
    results = col.aggregate(pipeline)
    i = 0
    for result in results:
        i+=1
        print(str(i)+". "+str(result["DanhSachChuyens"]["CreatedAt"])+", loại chuyển:"+result["LoaiDieuChuyen"]+", nghiệp vụ chuyển: "+str(result["NghiepVu"]))


def timconbo(sotai):
    startTime = time.time()
    print("Số tai tìm: "+sotai)
    conbo= db.bonhaptrai_find({"$text":{"$search":sotai}})
    for bo in conbo:
        print("Số tai tìm thấy: "+bo["SoTai"]) 
    endTime = time.time()
    print("Thời gian: "+str(endTime-startTime))


def findBoTheoOChuong(ochuong):
    match_condition = {
                    "OChuongHienTai.TenOChuong": ochuong,
            }
    pipeline = [
        {
            "$match": match_condition
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$Bo.SoTai"},
                "danhsachid": {"$push": "$_id"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
                "danhsachid":1,
            }
        },
    ]
    startTime = time.time()
    results = db.bonhaptrai_aggregate(pipeline)
    reportName = "Tổng số bo o o chuong " + ochuong+": "
    danhsachid = []
    soluong = None

    for result in results:
        if result != None:
            soluong = result["soluong"]
            danhsachid = result["danhsachid"]
            print(len(danhsachid))
            for _id in danhsachid:
                print(str(_id))
    endTime = time.time()
    print(reportName + ":    " + str(soluong))
    print("Tong thoi gian: "+str(endTime-startTime))
    return danhsachid