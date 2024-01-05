from pymongo import MongoClient
from datetime import datetime, timedelta
import time
import constants
from client import db, test_result_collection

date_format = "%Y-%m-%d"

startTime = time.time()


test_result_collection.baocaothang.delete_many({"LoaiBaoCao":"Vaccine"})

testResultDocument = test_result_collection.baocaothang.find_one({"LoaiBaoCao": "Vaccine"})
if testResultDocument is None:
    testResultDocument = test_result_collection.baocaothang.insert_one(
        {"LoaiBaoCao": "Vaccine", "CreatedAt": datetime.now(), "KetQua": []}
    )
    testResultId = testResultDocument.inserted_id
else:
    testResultId = testResultDocument["_id"]
    test_result_collection.baocaothang.update_one(
        {"_id": testResultId}, {"$set": {"KetQua": [], "UpdatedAt": datetime.now()}}
    )


giaiDoanBoVoBeo = ["BoVoBeoNho", "BoVoBeoTrung", "BoVoBeoLon"]

giaiDoanBoChoPhoi = ["BoChoPhoi", "BoHauBiChoPhoi"]

tatCaNhomBoSong = {
    "tennhom": "bò",
    "danhsach": ["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be", None],
}

boKhongOTrai = {
    "tennhom": "bò không ở trại",
    "danhsach": ["XuatBan", "LoaiThai", "BeChet"],
}

gioiTinhTatCa = {
    "tennhom": "",
    "danhsach": ["Đực", "Cái", "Không xác định", None, ""],
}






# 1	Tổng số bò đã được tiêm vaccine
def tongSo_boDuocTiemVaccine(startdate,enddate,nhomVaccine):
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)+timedelta(days=1)
    nhomvaccineID = None
    nhomvaccine = db.nhomvaccine_find({"MaNhomVaccine":nhomVaccine})
    if nhomvaccine is not None:
        nhomvaccineID = nhomvaccine["_id"]
    else:
        print("Không tìm thấy nhóm vaccine "+nhomVaccine)

    pipeline = [
        {
            "$match": {
                    "NgayThucHien": {"$gte": startDate},
                    "NgayThucHien": {"$lt": endDate},
                    "DanhMucVaccine.NhomVaccineModel._id": nhomvaccineID,
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
    danhsachsotai = ""
    soluong = None

    results = db.vaccine_aggregate(pipeline)
    reportName = "Tổng số bò được tiêm vaccine "+nhomVaccine["ma"]
    print(reportName)
    for result in results:
        if result != None:
            danhsachsotai = result["danhsachsotaijoined"]
            soluong = result["soluong"]
    test_result = {
        "NoiDung":"Tổng số bò được tiêm vaccine",
        "CreatedAt":datetime.now(),
        "SoLuong":soluong,
        "NgayBatDau":startDate,
        "NgayKetThuc":endDate,
        "DanhSachSoTai":danhsachsotai
    }
    test_result_collection.baocaothang.update_one(
        {"_id": testResultId}, {"$push": {"KetQua": test_result}}
    )
    print(reportName +": "+ str(soluong))

# 2. Tổng số bò đủ điều kiện tiêm vaccine (trừ tụ huyết trùng)
def tongSo_boDuDieuKienTiem(workingDate,nhomVaccine,nhombo = tatCaNhomBoSong):
    dateToCheck = datetime.strptime(workingDate, date_format)+timedelta(days=1)
    nhomvaccineID = None
    nhomvaccine = db.nhomvaccine_find({"MaNhomVaccine":nhomVaccine})
    if nhomvaccine is not None:
        nhomvaccineID = nhomvaccine["_id"]
    else:
        print("Không tìm thấy nhóm vaccine "+nhomVaccine)

    print(nhomVaccine["ma"])

    pipelineLieuTrinh = [
        {"$match": {"NhomVaccineModel._id":nhomvaccineID}},
        {"$project":{"SoNgayTuoi":1,"NhomVaccineModel.ChuKyTiem":1}},
        {"$group":{
            "_id":None,
            "ngaytuois":{"$push":"$SoNgayTuoi"},
            "chuky":{"$max":"$NhomVaccineModel.ChuKyTiem"}
        }}
    ]

    ngaytuoiminlieutrinh = 0
    ngaytuoimaxlieutrinh = 0
    solieutrinh = 0
    chukytiem = 0
    danhsachngaylieutrinh = []

    lieutrinhresult = db.lieutrinhvaccine.aggregate(pipelineLieuTrinh)

    for lieutrinh in lieutrinhresult:
        danhsachngaylieutrinh = lieutrinh["ngaytuois"]
        danhsachngaylieutrinh.sort()
        print(danhsachngaylieutrinh)
        ngaytuoiminlieutrinh = danhsachngaylieutrinh[0]
        ngaytuoimaxlieutrinh = danhsachngaylieutrinh[-1]
        solieutrinh = len(danhsachngaylieutrinh)
        chukytiem = lieutrinh["chuky"]
        print("Ngày tuổi nhỏ nhất để tiêm: "+str(ngaytuoiminlieutrinh))
        print("Ngày tuổi cuối cùng trước khi lặp lại: "+str(ngaytuoimaxlieutrinh))
        print("Số bước liệu trình: "+str(solieutrinh))
        print("Chu kỳ tiêm: "+str(chukytiem))
        

    datethreshold = dateToCheck - timedelta(days = ngaytuoiminlieutrinh)
    datethreshold2 = dateToCheck - timedelta(days = chukytiem)
    datethreshold3 = dateToCheck - timedelta(days = ngaytuoimaxlieutrinh)
    ngaymangthaitoida = dateToCheck - timedelta(days = 240)
    ngaydetoithieu = dateToCheck - timedelta(days = 15)


    pipeline = [
        {"$match":{"$or":[{"$and":[{"NhomBo":{"$in":nhombo["danhsach"]}},{"NgaySinh":{"$ne":None}},{"NgaySinh":{"$lt":datethreshold}}]},{"$and":[{"NhomBo":{"$in":nhombo["danhsach"]}},{"NgaySinh":None}]}]}},
        {"$match":{"$or":[
        # Bò nhập chưa tiêm vaccine lần nào
        {"$and":[{"NgaySinh":None},{"$or":[{"LichSuTiemVaccines":{"$size":0}},{"LichSuTiemVaccines":{"$not":{"$elemMatch":{"NgayThucHien":{"$gte":datethreshold2},"DanhMucVaccine.NhomVaccineModel._id":nhomVaccine["_id"]}}}}]}]},
        # Bê tới/quá ngày tiêm lần 1 nhưng chưa tiêm lần nào
        {"$and":[{"NgaySinh":{"$ne":None}},{"LichSuTiemVaccines":{"$size":0}}]},
        # Bê tới/quá ngày tiêm lần 2 nhưng chưa tiêm lần nào
        {"$and":[{"NgaySinh":{"$ne":None}},{"NgaySinh":{"$lte":datethreshold3}},{"LichSuTiemVaccines":{"$size":0}}]},
        # Bê tới/quá ngày tiêm lần 2 nhưng chỉ tiêm 1 lần
        {"$and":[{"NgaySinh":{"$ne":None}},{"NgaySinh":{"$lte":datethreshold3}},{"LichSuTiemVaccines":{"$size":1}}]},
        # Bê tới/quá ngày nhắc lại nhưng chưa tiêm
        {"$and":[{"NgaySinh":{"$ne":None}},{"$or":[{"LichSuTiemVaccines":{"$size":0}},{"LichSuTiemVaccines":{"$not":{"$elemMatch":{"NgayThucHien":{"$gte":datethreshold2},"DanhMucVaccine.NhomVaccineModel._id":nhomVaccine["_id"]}}}}]}]},
        ]}},
        # Trừ bò mang thai trên 240 ngày và bò đẻ dưới 15 ngày
        {"$match":{
                "$nor":[{"$and":[
                    {"NhomBo":"BoMangThaiLon"},
                    {"ThongTinKhamThais.-1.NgayMangThai":{"$lt":ngaymangthaitoida}}
                ]},
                {"$and":[
                    {"NhomBo":"BoNuoiConNho"},
                    {"ThongTinSinhSans.-1.NgaySinh":{"$gt":ngaydetoithieu}}
                ]}]
        }},
        # Trừ bò nằm trong phiếu điều trị thú y
        {"$lookup":{
            "from":"DieuTriBoBenh",
            "localField":"SoTai",
            "foreignField":"Bo.SoTai",
            "let":{"ngayKham":"$NgayKham"},
            "pipeline":[
            {"$match":{"TinhTrangDieuTri":{"$in":["ChuaKham","DaKham","DangDieuTri"]}}},
            {"$sort":{"NgayNhapVien":-1}},
            ],
            "as":"dieutri"
        }},
        {"$match":{"dieutri":{"$size":0}}},
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
    results = db.bonhaptrai_aggregate(pipeline)
    reportName = "Tong so luong bo du dieu kien tiem "+nhomVaccine["ma"]
    print(reportName)
    for result in results:
        print("   So luong:" + str(result["soLuong"]))
        if result != None:    
            test_result = {
                "LoaiBaoCao":"Vaccine",
                "NoiDung":"Tổng số bò đủ điều kiện tiêm vaccine",
                "CreatedAt":datetime.now(),
                "TenVaccine":nhomVaccine["ma"],
                "SoLuong":result["soLuong"],
                "NgayKiemTra":dateToCheck,
                "DanhSachSoTai":result["danhsachsotaijoined"]
            }
            test_result_collection.baocaothang.insert_one(test_result)

def tongSo_boDuDieuKienTiem_THT(workingDate, nhomVaccine,nhombo = tatCaNhomBoSong):
    dateToCheck = datetime.strptime(workingDate, date_format)

    print(nhomVaccine["ma"])

    pipelineLieuTrinh = [
        {"$match": {"NhomVaccineModel._id":nhomVaccine["_id"]}},
        {"$project":{"SoNgayTuoi":1,"NhomVaccineModel.ChuKyTiem":1}},
        {"$group":{
            "_id":None,
            "ngaytuois":{"$push":"$SoNgayTuoi"},
            "chuky":{"$max":"$NhomVaccineModel.ChuKyTiem"}
        }}
    ]

    ngaytuoiminlieutrinh = 0
    ngaytuoimaxlieutrinh = 0
    solieutrinh = 0
    chukytiem = 120
    danhsachngaylieutrinh = []

    lieutrinhresult = db.lieutrinhvaccine.aggregate(pipelineLieuTrinh)

    for lieutrinh in lieutrinhresult:
        danhsachngaylieutrinh = lieutrinh["ngaytuois"]
        danhsachngaylieutrinh.sort()
        print(danhsachngaylieutrinh)
        ngaytuoiminlieutrinh = danhsachngaylieutrinh[0]
        ngaytuoimaxlieutrinh = danhsachngaylieutrinh[-1]
        solieutrinh = len(danhsachngaylieutrinh)
        print("Ngày tuổi nhỏ nhất để tiêm: "+str(ngaytuoiminlieutrinh))
        print("Ngày tuổi cuối cùng trước khi lặp lại: "+str(ngaytuoimaxlieutrinh))
        print("Số bước liệu trình: "+str(solieutrinh))
        print("Chu kỳ tiêm: "+str(chukytiem))
        

    datethreshold = dateToCheck - timedelta(days = ngaytuoiminlieutrinh)
    print(datethreshold)
    datethreshold2 = dateToCheck - timedelta(days = chukytiem)
    print(datethreshold2)
    datethreshold3 = dateToCheck - timedelta(days = ngaytuoimaxlieutrinh)
    print(datethreshold3)
    ngaymangthaitoida = dateToCheck - timedelta(days = 240)
    print(ngaymangthaitoida)
    ngaydetoithieu = dateToCheck - timedelta(days = 15)
    print(ngaydetoithieu)


    pipeline = [
        {"$match":{"$or":[{"$and":[{"NhomBo":{"$in":nhombo["danhsach"]}},{"NgaySinh":{"$ne":None}},{"NgaySinh":{"$lte":datethreshold}}]},{"$and":[{"NhomBo":{"$in":nhombo["danhsach"]}},{"NgaySinh":None}]}]}},
        {"$match":{"$or":[
        # Bò nhập chưa tiêm vaccine lần nào
        {"$and":[{"NgaySinh":None},{"$or":[{"LichSuTiemVaccines":{"$size":0}},{"LichSuTiemVaccines":{"$not":{"$elemMatch":{"NgayThucHien":{"$gte":datethreshold2},"DanhMucVaccine.NhomVaccineModel._id":nhomVaccine["_id"]}}}}]}]},
        # Bê tới/quá ngày tiêm lần 1 nhưng chưa tiêm lần nào
        {"$and":[{"NgaySinh":{"$ne":None}},{"LichSuTiemVaccines":{"$size":0}}]},
        # Bê tới/quá ngày tiêm lần 2 nhưng chưa tiêm lần nào
        {"$and":[{"NgaySinh":{"$ne":None}},{"NgaySinh":{"$lte":datethreshold3}},{"LichSuTiemVaccines":{"$size":0}}]},
        # Bê tới/quá ngày tiêm lần 2 nhưng chỉ tiêm 1 lần
        {"$and":[{"NgaySinh":{"$ne":None}},{"NgaySinh":{"$lte":datethreshold3}},{"LichSuTiemVaccines":{"$size":1}}]},
        # Bê tới/quá ngày nhắc lại nhưng chưa tiêm
        {"$and":[{"NgaySinh":{"$ne":None}},{"$or":[{"LichSuTiemVaccines":{"$size":0}},{"LichSuTiemVaccines":{"$not":{"$elemMatch":{"NgayThucHien":{"$gte":datethreshold2},"DanhMucVaccine.NhomVaccineModel._id":nhomVaccine["_id"]}}}}]}]},
        ]}},
        # Trừ bò mang thai trên 240 ngày và đẻ dưới 15 ngày
        {"$match":{
            "$nor":[{"$and":[
                {"NhomBo":"BoMangThaiLon"},
                {"ThongTinKhamThais.-1.NgayMangThai":{"$lt":ngaymangthaitoida}}
            ]},
            {"$and":[
                {"NhomBo":"BoNuoiConNho"},
                {"ThongTinSinhSans.-1.NgaySinh":{"$gt":ngaydetoithieu}}
            ]}]
        }},
        # Trừ bò nằm trong phiếu điều trị thú y

        # {"$lookup":{
        #     "from":"DieuTriBoBenh",
        #     "localField":"SoTai",
        #     "foreignField":"Bo.SoTai",
        #     "let":{"ngayKham":"$NgayKham"},
        #     "pipeline":[
        #     {"$match":{"TinhTrangDieuTri":{"$in":["ChuaKham","DaKham","DangDieuTri"]}}},
        #     {"$sort":{"NgayNhapVien":-1}},
        #     ],
        #     "as":"dieutri"
        # }},
        # {"$match":{"dieutri":{"$size":0}}},
        # {"$sort":"SoTai"},
        {"$match":{
            "$not":{
                "LichSuDieuTris":{
                    "$elemMatch":{
                        "TinhTrangDieuTri":{
                            "$in":["DangDieuTri","DaKham","ChuaKham"]
                        }
                    }
                }
            }
        }},
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
    results = db.bonhaptrai.aggregate(pipeline)
    reportName = "Tong so luong bo du dieu kien tiem "+nhomVaccine["ma"]
    print(reportName)
    for result in results:
        print("   So luong:" + str(result["soLuong"]))
        if result != None:
            test_result = {
                "LoaiBaoCao":"Vaccine",
                "NoiDung":"Tổng số bò đủ điều kiện tiêm vaccine",
                "CreatedAt":datetime.now(),
                "TenVaccine":nhomVaccine["ma"],
                "SoLuong":result["soLuong"],
                "NgayKiemTra":dateToCheck,
                "DanhSachSoTai":result["danhsachsotaijoined"]
            }
            test_result_collection.baocaothang.insert_one(test_result)



# Tổng số bò đủ điều kiện tiêm vaccine (một cách tổng quát)

def tongSoBoDuDieuKienTiem_general(workingDate,nhomVaccine):
    dateToCheck = datetime.strptime(workingDate, date_format)+timedelta(days=1)
    nhomvaccineID = None
    nhomvaccine = db.nhomvaccine_find({"MaNhomVaccine":nhomVaccine})
    ngaytuoitoithieu = 0
    lieutrinhtimthays = db.lieutrinhvaccine_aggregate([
        {"$match":{
            "NhomVaccineModel._id":nhomvaccineID
        }},
        {"$group":{
            "_id":"null",
            "minage":{"$min":"$SoNgayTuoi"},
            "maxage":{"$max":"$SoNgayTuoi"},
        }},
        
    ])
    if nhomvaccine is not None:
        nhomvaccineID = nhomvaccine["_id"]
    else:
        print("Không tìm thấy nhóm vaccine "+nhomVaccine)
    # Xét danh sách bò, tìm những con đủ ngày tiêm vaccine
    pipeline = [
        {"$match":{
            # Không nằm trong nhóm bò k còn ở trại
            "NhomBo":{
                "$nin":boKhongOTrai,
            }
        }},
    ]

    reportName = "Tổng số bò được tiêm vaccine "+nhomVaccine["ma"]
    results = None
    for result in results:
        if result != None:
            danhsachsotai = result["danhsachsotaijoined"]
            soluong = result["soluong"]
    test_result = {
        "NoiDung":reportName,
        "CreatedAt":datetime.now(),
        "SoLuong":soluong,
        "NgayKiemTra":dateToCheck,
        "DanhSachSoTai":danhsachsotai
    }
    test_result_collection.baocaothang.update_one(
        {"_id": testResultId}, {"$push": {"KetQua": test_result}}
    )
    print(reportName +": "+ str(soluong))