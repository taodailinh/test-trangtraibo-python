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


# 1	Tổng số bò đực giống đã được đề xuất thanh lý	
# 2	Bò không đủ tiêu chuẩn xử lý sinh sản	
# 3	Tổng số bò được xử lý hormone sinh sản ngày 0	
# 4	Tổng số bò được xử lý hormone sinh sản 7	
# 5	Tổng số bò được xử lý hormone sinh sản 9	
# 6	Tổng số bò được xử lý hormone sinh sản 10	
# 7	Tổng số bò được gieo tinh nhân tạo từ bò lên giống tự nhiên (không xử lý sinh sản)	
# 8	Tổng số bò được ghép đôi phối giống với bò đực giống	
# 9	Tổng số bò gieo tinh nhân tạo được khám thai: (Chỉ tiêu đánh gia các chỉ tiêu dưới)	
# 10	Tổng số bò xử lý sinh sản có thai	
# 11	Tổng số bò xử lý sinh sản không có thai	
# 12	Tổng số bò lên giống tự nhiên được gieo tinh nhân tạo có thai	
# 13	Tổng số bò lên giống tự nhiên được gieo tinh nhân tạo không có thai	
# 14	Tổng số bò ghép đực được khám thai	
# 15	Tổng số bò ghép đực có thai	
# 16	Tổng số bò ghép đực không có thai	
# 17	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 1	
# 18	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 2	
# 19	Tỷ lệ đậu thai do gieo tinh nhân tạo lần 3	
# 20	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 1	
# 21	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 2	
# 22	Tỷ lệ đậu thai do gieo tinh nhân tạo của giống bò Brahman lần 3	
# 23	Tỷ lệ đậu thai do ghép đực của giống bò Brahman	
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
