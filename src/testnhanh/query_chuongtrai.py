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
    "danhsach": ["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be"],
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

def duLieuChuongTrai():
    match_condition={"NhomBo":{"$in":tatCaNhomBo["danhsach"]}}
    pipeline = [
        {"$match":match_condition},
        {"$group":{
            "_id":"$OChuongHienTai._id",
            "soluongbe":{
                "$sum":{
                    "$cond":[{"$eq":["$NhomBo","Be"]},1,0]
                }
            },
            "tongsoluongbobe":{"$count":{}},
            "ochuong":{"$first":"$OChuongHienTai.TenOChuong"}

        }},
        {"$project":{
            "_id":1,
            "soluongbe":1,
            "tongsoluongbobe":1,
            "ochuong":1,
        }}
    ]
    startTime = time.time()
    chuongtrai = []
    soluongochuong = 0
    results = db.bonhaptrai_aggregate(pipeline)
    for result in results:
        if result is not None:
            soluongochuong+=1
            print(str(soluongochuong)+". O chuong: "+result["ochuong"])
            print("So luong bo: "+str(result["tongsoluongbobe"] - result["soluongbe"]))
            print("So luong be: "+str(result["soluongbe"]))
            print("Tong so luong bo be: "+str(result["tongsoluongbobe"]))

  
            # chuongtrai.append({
            #     "_id":result["_id"],
            #     "soluongbe":result["soluongbe"],
            #     "tongsoluongbobe":result["tongsoluongbobe"],
            #     "soluongbo":result["tongsoluongbobe"] - result["soluongbe"],
            # })

    test_result = {
        "NoiDung": "Thong tin o chuong",
        "CreatedAt":datetime.now(),
        "ChuongTrai": chuongtrai,
    }
    # test_result_collection.query.update_one(
    #     {"_id": testResultId}, {"$push": {"KetQua": test_result}}
    # )
    print("So o chuong tim thay: "+str(soluongochuong))
    endTime = time.time()
    print("Tong thoi gian: "+str(endTime-startTime))
