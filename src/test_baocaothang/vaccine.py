from pymongo import MongoClient
from datetime import datetime, timedelta
import time

date_format = "%Y-%m-%d"

startTime = time.time()

giaiDoanBoVoBeo = ["BoVoBeoNho", "BoVoBeoTrung", "BoVoBeoLon"]

giaiDoanBoChoPhoi = ["BoChoPhoi", "BoHauBiChoPhoi"]

tatCaNhomBoSong = {
    "tennhom": "bò",
    "danhsach": ["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be", None],
}

gioiTinhTatCa = {
    "tennhom": "",
    "danhsach": ["Đực", "Cái", "Không xác định", None, ""],
}






# 1	Tổng số bò đã được tiêm vaccine
def tongSo_boDuocTiemVaccine(
    client: MongoClient, dbName, collectionName, startdate, enddate, excelWriter,nhomVaccine
):
    db = client[dbName]
    col = db[collectionName]
    startDate = datetime.strptime(startdate, date_format)
    endDate = datetime.strptime(enddate, date_format)
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"NgayThucHien": {"$gte": startDate}},
                    {"NgayThucHien": {"$lte": endDate}},
                    {"DanhMucVaccine.NhomVaccineModel._id": nhomVaccine["_id"]},
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
    reportName = "Tổng số bò được tiêm vaccine "+nhomVaccine["ma"]
    print(reportName)
    for result in results:
        print("   So luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)


# 2. Tổng số bò đủ điều kiện tiêm vaccine (trừ tụ huyết trùng)
def tongSo_boDuDieuKienTiem(
    client: MongoClient, dbName, bonhaptrai, tiemVaccineCollection, nhomVaccineCollection, lieutrinhVaccineCollection, workingDate, excelWriter,nhomVaccine
,nhombo = tatCaNhomBoSong):
    db = client[dbName]
    bo = db[bonhaptrai]
    tiemCol = db[tiemVaccineCollection]
    nhomVacCol = db[nhomVaccineCollection]
    lieutrinhCol = db[lieutrinhVaccineCollection]
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
    chukytiem = 0
    danhsachngaylieutrinh = []

    lieutrinhresult = lieutrinhCol.aggregate(pipelineLieuTrinh)

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
    results = bo.aggregate(pipeline)
    reportName = "Tong so luong bo du dieu kien tiem "+nhomVaccine["ma"]
    print(reportName)
    for result in results:
        print("   So luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)

def tongSo_boDuDieuKienTiem_THT(
    client: MongoClient, dbName, bonhaptrai, tiemVaccineCollection, nhomVaccineCollection, lieutrinhVaccineCollection, workingDate, excelWriter,nhomVaccine
,nhombo = tatCaNhomBoSong):
    db = client[dbName]
    bo = db[bonhaptrai]
    tiemCol = db[tiemVaccineCollection]
    nhomVacCol = db[nhomVaccineCollection]
    lieutrinhCol = db[lieutrinhVaccineCollection]
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

    lieutrinhresult = lieutrinhCol.aggregate(pipelineLieuTrinh)

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
        # Trừ bò mang thai trên 240 ngày
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
        # Trừ bò đẻ dưới 15 ngày
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
        {"$sort":"SoTai"},
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
    results = bo.aggregate(pipeline)
    reportName = "Tong so luong bo du dieu kien tiem "+nhomVaccine["ma"]
    print(reportName)
    for result in results:
        print("   So luong:" + str(result["soLuong"]))
        row = [reportName, result["soLuong"], result["danhsachsotaijoined"]]
        excelWriter.append(row)

