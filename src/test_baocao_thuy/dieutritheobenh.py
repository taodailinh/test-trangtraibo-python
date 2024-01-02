from pymongo import MongoClient
from datetime import datetime
import time

date_format = "%Y-%m-%d"


def codieutri(client:MongoClient,dbName,collectionName,startdate:str,enddate:str,worksheet):
    db=client[dbName]
    startDate = datetime.strptime(startdate,date_format)
    endDate = datetime.strptime(enddate,date_format)
    collection_danhmucbenh = db["NhomBenh"]
    collection = db[collectionName]
#     pipeline_danhsachbenh = [
#     {
#         '$group': {
#             '_id': 'null', 
#             'nhombenh': {
#                 '$addToSet': '$TenNhomBenh'
#             }
#         }
#     },
#     {"$unwind":"nhombenh"}
# ]
    # danhsachbenh = list(collection_danhmucbenh.aggregate(pipeline_danhsachbenh))
    pipeline = [
        {"$match":{"$expr":{"$gte":["$NgayDieuTri",startDate]}}},
        {"$match":{"$expr":{"$or":[{"$gt":[{"$size":"$DieuTris"},0]},{"$ne":[{"$size":"$DieuTriBenh.NguoiThucHiens"},0]}]}}},
        # {"$unwind":"$DieuTris.PhacDoDieuTris.ChiTietPhacDos"},
        # {"$unwind":"$DieuTriBenh.PhacDoDieuTris"},
        # {"$unwind":"$DieuTriBenh.PhacDoDieuTris.ChiTietPhacDos"},
        # {"$unwind":"$DieuTriBenh.PhacDoDieuTriHienTai.ChiTietPhacDos"},
        {"$match":{"$or":[
            {"DieuTriBenh.PhacDoDieuTriHienTai.ChiTietPhacDos":{
                        "$elemMatch":{
                        "NgayThucHien":{
                            "$gte":startDate,
                            "$lte":endDate
                    }
                }
            }},
            {"DieuTriBenh.PhacDoDieuTris":{
                        "$elemMatch":{
                        "ChiTietPhacDos":{
                            "$elemMatch":{
                                "NgayThucHien":
                                {"$gte":startDate,
                                 "$lte":endDate}
                    }
                    }
                }
            }},
            {"DieuTris":{
                "$elemMatch":{
                    "PhacDoDieuTris":{
                        "$elemMatch":{
                        "ChiTietPhacDos":{
                            "$elemMatch":{
                                "NgayThucHien":
                                {"$gte":startDate,
                                 "$lte":endDate}
                            }
                        }
                    }
                    }
                }
            }},
            {"DieuTris":{
                "$elemMatch":{
                    "PhacDoDieuTriHienTai.ChiTietPhacDos":{
                            "$elemMatch":{
                                "NgayThucHien":
                                {"$gte":startDate,
                                 "$lte":endDate}
                    }
                    }
                }
            }}
        ]}},
        # {"$match":{
        #     "$expr":
        #     {"$or":[
        #         {"DieuTris.PhacDoDieuTris.ChiTietPhacDos.NgayThucHien":{
        #             "$elemMatch":{
        #                 "$gte":startDate,
        #                 "$lte":endDate
        #             }
        #         }},
        #     ]}
        # }},
        {"$group":
            {"_id":None,
            "soluong":{"$count":{}},
            "danhsachsotai":{"$push":"$Bo.SoTai"}
             }
        },
        {"$project":{
            "_id":0,
            "soluong":1,
            "danhsachsotaijoined":{
                "$reduce":{
                    "input":"$danhsachsotai",
                    "initialValue":"",
                    "in":{
                        "$concat":["$$value",
                         {"$cond":[{"$eq":["$$value",""]},"",";"]},
                         "$$this"
                         ]
                    }
                }
            }
        }}
    ]
    results = collection.aggregate(pipeline)
    title = "Số lượng bò có điều trị"
    for result in results:
        print(title + " :" + str(result["soluong"]))
        worksheet.append([title,result["soluong"],result["danhsachsotaijoined"]])


# Bò đang điều trị
def dangdieutri(client:MongoClient,dbName,collectionName):
    db=client[dbName]
    collection = db[collectionName]
    collection_testresult = client["Linh_Test"]["Query_ThuY"]
    pipeline = [
        {"$match":{
            "TinhTrangDieuTri":{
                "$in":["ChuaKham","DangDieuTri","DaKham"]
            }
        }},
        {"$group":
            {"_id":None,
            "soLuong":{"$count":{}},
            "danhsachsotai":{"$push":"$Bo.SoTai"}
             }
        },
        {"$project":{
            "_id":0,
            "soLuong":1,
            "danhsachsotaijoined":{
                "$reduce":{
                    "input":"$danhsachsotai",
                    "initialValue":"",
                    "in":{
                        "$concat":["$$value",
                         {"$cond":[{"$eq":["$$value",""]},"",";"]},
                         "$$this"
                         ]
                    }
                }
            }
        }}
    ]
    results = collection.aggregate(pipeline)
    title = "Số lượng bò có điều trị"
    for result in results:
        print(title + " :" + str(result["soLuong"]))
        if result != None:
            test_result = {
                "NoiDung":"Tổng số lượng bò đang điều trị",
                "ThoiDiem": datetime.now(),
                "SoLuong":result["soLuong"],
                "DanhSachSoTai":result["danhsachsotaijoined"]

            }
        collection_testresult.insert_one(test_result)

def khongdangdieutri(client:MongoClient,dbName,collectionName):
    db=client[dbName]
    collection = db[collectionName]
    collection_testresult = client["Linh_Test"]["Query_ThuY"]
    pipeline = [
        {"$match":{
            "NhomBo":{
                "$in":["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be"]
            },
            "LichSuDieuTris":{
                "$not":{
                    "$elemMatch":{
                        "TinhTrangDieuTri":{
                            "$in":["ChuaKham","DangDieuTri","DaKham"]
                        }
                    }
                }
            }
        }},
        {"$group":
            {"_id":None,
            "soLuong":{"$count":{}},
            "danhsachsotai":{"$push":"$SoTai"}
             }
        },
        {"$project":{
            "_id":0,
            "soLuong":1,
            "danhsachsotaijoined":{
                "$reduce":{
                    "input":"$danhsachsotai",
                    "initialValue":"",
                    "in":{
                        "$concat":["$$value",
                         {"$cond":[{"$eq":["$$value",""]},"",";"]},
                         "$$this"
                         ]
                    }
                }
            }
        }}
    ]
    results = collection.aggregate(pipeline)
    title = "Số lượng bò không đang điều trị"
    for result in results:
        print(title + " :" + str(result["soLuong"]))
        if result != None:
            test_result = {
                "NoiDung":"Tổng số lượng bò không đang điều trị",
                "ThoiDiem": datetime.now(),
                "SoLuong":result["soLuong"],
                "DanhSachSoTai":result["danhsachsotaijoined"]

            }
        collection_testresult.insert_one(test_result)