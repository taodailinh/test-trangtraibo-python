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
    dateToCheck = datetime.strptime(workingDate, date_format)
    nhomvaccineID = None
    nhomvaccine = db.nhomvaccine_find({"MaNhomVaccine":nhomVaccine})
    ngaysinhtoithieu = None
    if nhomvaccine is not None:
        nhomvaccineID = nhomvaccine["_id"]
        print("Nhóm vaccine: "+nhomvaccine["TenNhomVaccine"])
    else:
        print("Không tìm thấy nhóm vaccine "+nhomVaccine)
    
    solieutrinh = 1
    ngaytuoilieutrinh1 = 0
    ngaytuoilieutrinh2 = None
    ngaysinhtoithieu_lieutrinh1 = None
    ngaysinhtoithieu_lieutrinh2 = None
    lieutrinhtimthays = db.lieutrinhvaccine_aggregate([
        {"$match":{
            "NhomVaccineModel._id":nhomvaccineID
        }},
        {"$group":{
            "_id":"null",
            "solieutrinh":{"$count":{}},
            "minage":{"$min":"$SoNgayTuoi"},
            "maxage":{"$max":"$SoNgayTuoi"},
        }},
        {"$project":{
            "_id":0,
            "solieutrinh":1,
            "minage":1,
            "maxage":1
        }}
    ])
    for lieutrinhtimthay in lieutrinhtimthays:
        if lieutrinhtimthay is not None:
            ngaytuoilieutrinh1 = lieutrinhtimthay["minage"]
            ngaytuoilieutrinh2 = lieutrinhtimthay["maxage"]
            solieutrinh = lieutrinhtimthay["solieutrinh"]
            ngaysinhtoithieu_lieutrinh1 = dateToCheck - timedelta(days=ngaytuoilieutrinh1)
            ngaysinhtoithieu_lieutrinh2 = dateToCheck - timedelta(days=ngaytuoilieutrinh2)

        else:
            print("Không tìm thấy liệu trình đối với nhóm vaccine đã chọn")
    print("Ngày kiểm tra: "+str(dateToCheck))
    print("Số liệu trình: "+str(solieutrinh))
    print("Ngày tuổi tối thiểu để tiêm liệu trình 1: "+str(ngaysinhtoithieu_lieutrinh1))
    print("Ngày tuổi tối thiểu để tiêm liệu trình 2: "+str(ngaysinhtoithieu_lieutrinh2))
    dieukientiemvaccine = {}
    if solieutrinh == 1:
        dieukientiemvaccine = {                
                "$or":[
                # Bò nhập chưa tiêm vaccine lần nào
                {"NgaySinh":None,"thongtintiem.solantiem":0},
                # Bê tới quá ngày tiêm lần 1 nhưng chưa tiêm lần nào
                {"NgaySinh":{"$ne":None,"$lte":ngaysinhtoithieu_lieutrinh1},"thongtintiem.solantiem":0},
                # Bò/bê quá ngày tiêm nhắc lại nhưng chưa được tiêm
                {"thongtintiem.lantiemcuoi.NgayThucHienTiepTheo":{"$lte":dateToCheck}}
                ]
        }
    else:
        dieukientiemvaccine = {                
                "$or":[
                # Bò nhập chưa tiêm vaccine lần nào
                {"NgaySinh":None,"thongtintiem":None},
                # Bê tới quá ngày tiêm lần 1 nhưng chưa tiêm lần nào
                {"NgaySinh":{"$ne":None,"$lte":ngaysinhtoithieu_lieutrinh1},"thongtintiem":None},
                # Bê tới/quá ngày tiêm lần 2 nhưng chưa tiêm lần nào
                {"NgaySinh":{"$ne":None,"$lte":ngaysinhtoithieu_lieutrinh2},"thongtintiem":None},
                # Bê tới/quá ngày tiêm lần 2 nhưng mới tiêm 1 lần
                {"NgaySinh":{"$ne":None,"$lte":ngaysinhtoithieu_lieutrinh2},"thongtintiem.solantiem":1},
                # Bò/bê quá ngày tiêm nhắc lại nhưng chưa được tiêm
                {"thongtintiem.NgayTiemTiepTheo":{"$lte":dateToCheck}}
                ]
        }


    # Xét danh sách bò, tìm những con đủ ngày tiêm vaccine
    pipeline = [
        {"$match":{
            # Không nằm trong nhóm bò k còn ở trại
            "NhomBo":{
                "$nin":boKhongOTrai["danhsach"],
            },
            #Không nằm trong danh sách loại trừ
        }},
        # Lấy lịch sử tiêm vaccine và ngày tiêm gần nhất
        {"$lookup":{
            "from":"ThongTinTiemVaccine",
            "let":{"boId":"$_id"},
            "pipeline":[
                {"$match":{
                    "$expr":
                        {"$and":
                            [{"$eq":["$Bo._id","$$boId"]},
                            {"$eq":["$DaHoanThanh",True]},
                            {"$eq":["$DanhMucVaccine.NhomVaccineModel._id",nhomvaccineID]},
                            ]
                        }
                }},
                {"$group":{
                    "_id":"null",
                    "solantiem":{"$count":{}},
                    "lantiem":{
                        "$push":{
                            "NgayThucHien":"$NgayThucHien",
                            "MaVaccine":"$DanhMucVaccine.MaVaccine",
                            "IDVaccine":"$DanhMucVaccine._id",
                            "NgayThucHienTiepTheo":"$NgayThucHienTiepTheo"
                        }
                    }
                }},
                {
                "$addFields": {
                    "lantiemcuoi": {
                        "$reduce": {
                            "input": "$lantiem",
                            "initialValue": {"NgayThucHien": None},
                            "in": {
                                "$cond": [
                                    {
                                        "$or": [
                                            {
                                                "$gt": [
                                                    "$$this.NgayThucHien",
                                                    "$$value.NgayThucHien",
                                                ]
                                            },
                                            {"$eq": ["$$value.NgayThucHien", None]},
                                        ]
                                    },
                                    "$$this",
                                    "$$value",
                                ]
                            },
                        }
                    }
                }
            },
            {"$lookup":{
                "from":"DanhMucVaccineModel",
                "localField":"lantiemcuoi.IDVaccine",
                "foreignField":"_id",
                "as":"songaybaohocuoi"
            }},
            {"$project":{
                "_id":0,
                "solantiem":1,
                "lantiemcuoi":1,
                "ngaytiemtieptheo":{
                    "$dateAdd":{"startDate":"$lantiemcuoi.NgayTiem","unit":"day","amount":"$songaybaohocuoi.ChuKyTiem"}
                }
            }},
            ],
            "as":"thongtintiem"
        }},
        {
            "$match":dieukientiemvaccine
        },
        {
            "$group": {
                "_id": "null",
                "soluong": {"$count": {}},
                "danhsachsotai": {"$push": "$SoTai"},
                "thongtintiemvaccine":{"$push":"$thongtintiem"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "soluong": 1,
                "thongtintiemvaccine":1,
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
    soluong = 0
    danhsachsotai = ""
    thongtintiemvaccine = [] 
    reportName = "Tổng số bò đủ điều kiện tiêm vaccine "+nhomVaccine
    results = db.bonhaptrai_aggregate(pipeline)
    for result in results:
        if result != None:
            danhsachsotai = result["danhsachsotaijoined"]
            soluong = result["soluong"]
            thongtintiemvaccine = result["thongtintiemvaccine"]
    test_result = {
        "NoiDung":reportName,
        "CreatedAt":datetime.now(),
        "SoLuong":soluong,
        "ThongTinTiemVaccine":thongtintiemvaccine,
        "NgayKiemTra":dateToCheck,
        "DanhSachSoTai":danhsachsotai
    }
    test_result_collection.baocaothang.update_one(
        {"_id": testResultId}, {"$push": {"KetQua": test_result}}
    )
    print(reportName +": "+ str(soluong))


def danhsachbodatiem(workingDate,nhomVaccine):
    nhomvaccineID = None
    nhomvaccine = db.nhomvaccine_find({"MaNhomVaccine":nhomVaccine})
    ngaysinhtoithieu = None
    if nhomvaccine is not None:
        nhomvaccineID = nhomvaccine["_id"]
        print("Nhóm vaccine: "+nhomvaccine["TenNhomVaccine"])
    else:
        print("Không tìm thấy nhóm vaccine "+nhomVaccine)
    
    solieutrinh = 1
    ngaytuoilieutrinh1 = 0
    ngaytuoilieutrinh2 = None
    ngaysinhtoithieu_lieutrinh1 = None
    ngaysinhtoithieu_lieutrinh2 = None
    lieutrinhtimthays = db.lieutrinhvaccine_aggregate([
        {"$match":{
            "NhomVaccineModel._id":nhomvaccineID
        }},
        {"$group":{
            "_id":"null",
            "solieutrinh":{"$count":{}},
            "minage":{"$min":"$SoNgayTuoi"},
            "maxage":{"$max":"$SoNgayTuoi"},
        }},
        {"$project":{
            "_id":0,
            "solieutrinh":1,
            "minage":1,
            "maxage":1
        }}
    ])
    for lieutrinhtimthay in lieutrinhtimthays:
        if lieutrinhtimthay is not None:
            ngaytuoilieutrinh1 = lieutrinhtimthay["minage"]
            ngaytuoilieutrinh2 = lieutrinhtimthay["maxage"]
            solieutrinh = lieutrinhtimthay["solieutrinh"]
            ngaysinhtoithieu_lieutrinh1 = workingDate - timedelta(days=ngaytuoilieutrinh1)
            ngaysinhtoithieu_lieutrinh2 = workingDate - timedelta(days=ngaytuoilieutrinh2)

        else:
            print("Không tìm thấy liệu trình đối với nhóm vaccine đã chọn")
    print("Ngày kiểm tra: "+str(workingDate))
    print("Số liệu trình: "+str(solieutrinh))
    print("Ngày tuổi tối thiểu để tiêm liệu trình 1: "+str(ngaysinhtoithieu_lieutrinh1))
    print("Ngày tuổi tối thiểu để tiêm liệu trình 2: "+str(ngaysinhtoithieu_lieutrinh2))
    dieukientiemvaccine = {}
    if solieutrinh == 1:
        dieukientiemvaccine = {                
                "$or":[
                # Bò nhập chưa tiêm vaccine lần nào
                {"NgaySinh":None,"thongtintiem.solantiem":0},
                # Bê tới quá ngày tiêm lần 1 nhưng chưa tiêm lần nào
                {"NgaySinh":{"$ne":None,"$lte":ngaysinhtoithieu_lieutrinh1},"thongtintiem.solantiem":0},
                # Bò/bê quá ngày tiêm nhắc lại nhưng chưa được tiêm
                {"thongtintiem.lantiemcuoi.NgayThucHienTiepTheo":{"$lte":workingDate}}
                ]
        }
    else:
        dieukientiemvaccine = {                
                "$or":[
                # Bò nhập chưa tiêm vaccine lần nào
                {"NgaySinh":None,"thongtintiem":None},
                # Bê tới quá ngày tiêm lần 1 nhưng chưa tiêm lần nào
                {"NgaySinh":{"$ne":None,"$lte":ngaysinhtoithieu_lieutrinh1},"thongtintiem":None},
                # Bê tới/quá ngày tiêm lần 2 nhưng chưa tiêm lần nào
                {"NgaySinh":{"$ne":None,"$lte":ngaysinhtoithieu_lieutrinh2},"thongtintiem":None},
                # Bê tới/quá ngày tiêm lần 2 nhưng mới tiêm 1 lần
                {"NgaySinh":{"$ne":None,"$lte":ngaysinhtoithieu_lieutrinh2},"thongtintiem.solantiem":1},
                # Bò/bê quá ngày tiêm nhắc lại nhưng chưa được tiêm
                {"thongtintiem.NgayTiemTiepTheo":{"$lte":workingDate}}
                ]
        }


    # Xét danh sách bò, tìm những con đủ ngày tiêm vaccine
    pipeline = [
        {"$match":{
            "DanhMucVaccine.NhomVaccineModel._id":nhomvaccineID
        }},
        {
            "$group": {
                "_id": "$Bo._id",
                "soluong": {"$sum": 1},
                "thongtintiemvaccine":{"$push":{"NgayThucHien":"$NgayThucHien","VaccineId":"$DanhMucVaccine._id"}}
            }
        },
        {
            "$set":{
                "lantiemcuoi":{
                    "$reduce":{
                        "input":"$thongtintiemvaccine",
                        "initialValue":{"NgayThucHien":None},
                        "in":{
                            "$cond":[
                                {"$or":[{"$gt":["$$this.NgayThucHien","$$value.NgayThucHien"]},{"$eq":["$$value.NgayThucHien",None]}]},"$$this","$$value"
                            ]
                        }
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "soluong": 1,
                "thongtintiemvaccine":1,
                "lantiemcuoi":1,
            }
        },
        {"$limit":10}
    ]
    soluong = 0
    thongtintiemvaccine = [] 
    reportName = "Tổng số bò đủ điều kiện tiêm vaccine "+nhomVaccine
    startTime = time.time()
    results = list(db.vaccine_aggregate(pipeline))
    endTime = time.time()
    print("So luong: "+str(len(results)))
    print("Hoàn tất tìm danh sách lịch sử tiêm trong thời gian: "+str(endTime-startTime))
    # test_result = {
    #     "NoiDung":reportName,
    #     "CreatedAt":datetime.now(),
    #     "SoLuong":soluong,
    #     "ThongTinTiemVaccine":thongtintiemvaccine,
    #     "NgayKiemTra":dateToCheck,
    #     "DanhSachSoTai":danhsachsotai
    # }
    # test_result_collection.baocaothang.update_one(
    #     {"_id": testResultId}, {"$push": {"KetQua": test_result}}
    # )
    print(reportName +": "+ str(soluong))
    return results

def danhsachvaccine(nhomvaccine):
    pipeline = [
        {"$match":{
            "NhomVaccineModel.MaNhomVaccine":nhomvaccine
        }},
        {"$project":
        {"_id":1,
        "ChuKyTiem":1}
        }
    ]
    startTime = time.time()
    results = list(db.loaivaccine_aggregate(pipeline))
    endTime = time.time()
    print("Hoàn tất tìm danh sách vaccine trong: "+str(endTime-startTime))
    return results

def conditionOfList(workingDate,item):
    return item["lantiemcuoi"]["NgayThucHienTiepTheo"] <= workingDate

def lichSuTiem(checkdate,nhomVaccine):
    workingDate = datetime.strptime(checkdate,date_format)
    a = time.time()
    botrongtrai = db.bonhaptrai_aggregate([
        {"$match":{
            "NhomBo":{"$in":["Bo","Be","BoChuyenVoBeo","BoDucGiong"]},
            "NgaySinh":{"$lt":workingDate-timedelta(days=3)}
        }},
        {"$project":{
            "_id":1,
            "SoTai":1
        }}
    ])
    b = time.time()
    print("Thời gian lấy danh sách bò: "+ str(b-a))
    dsvaccine = danhsachvaccine(nhomVaccine)
    bodatiem = danhsachbodatiem(workingDate,nhomVaccine)
    lookup_vaccine = {item['_id']: item for item in dsvaccine}
    startTime = time.time()
    for item in bodatiem:
        match_value = item['lantiemcuoi']["VaccineId"]
        if match_value in lookup_vaccine:
            vaccineInfo = lookup_vaccine[match_value]
            if "ChuKyTiem" in vaccineInfo:
        # Summing 'field_to_sum' from list_a and 'other_field' from list_b
                item['lantiemcuoi']["NgayThucHienTiepTheo"] = item['lantiemcuoi']["NgayThucHien"]+timedelta(days=lookup_vaccine[match_value]["ChuKyTiem"]) 
                print("id:"+item["_id"])
                print("Ngay tiem tiep theo:"+str(item["lantiemcuoi"]["NgayThucHienTiepTheo"]))
            else:
                print("Vaccine chưa có ngày tiêm: "+item["_id"])
    
    id_bodatiem = set(item["_id"] for item in bodatiem)

    joined_list = [item for item in botrongtrai if item['_id'] not in id_bodatiem] + [item for item in bodatiem if conditionOfList(workingDate,item)]

    for item in joined_list:
        print(item["_id"])

    endTime = time.time()
    print("Hoàn tất tính danh sách bò cần tiêm: "+str(endTime-startTime))

