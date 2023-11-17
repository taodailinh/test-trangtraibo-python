from datetime import datetime, timedelta
import testnhanh.query as query
import constants
import client
from client import db, test_result_collection, changeFarm
from pymongo import MongoClient
from openpyxl import Workbook

page = "https://test-trangtrai.aristqnu.com/"

user = "admin"
password = "admintest"

startDate = "2023-09-01"
endDate = "2023-09-30"

# Connect to mongodb
# client = MongoClient(constants.CONNECTION_STRING)
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
# db = constants.DB

# Create workbook log
# wb = Workbook()
# ws = wb.active
danhsachnhombo = ["XuatBan", "Bo", "Be"]

# Danh sách phân loại bò
beDuoi1thang = {"tennhom": "bê dưới 1 tháng", "danhsach": ["BeSinh"]}
beTheoMe = {"tennhom": "bê theo mẹ 1-4 tháng", "danhsach": ["BeTheoMe"]}
beCaiSua = {"tennhom": "bê theo mẹ 1-4 tháng", "danhsach": ["BeCaiSua"]}
boHauBi = {"tennhom": "bò hậu bị 9-12 tháng", "danhsach": ["BoHauBi"]}
boHauBiChoPhoi = {"tennhom": "bò hậu bị 13-18 tháng", "danhsach": ["BoHauBiChoPhoi"]}
boNuoiThitBCT9_12 = {
    "tennhom": "bò nuôi thịt BCT 9-12 tháng",
    "danhsach": ["BoNuoiThitBCT8_12"],
}
boNuoiThitBCT13_18 = {
    "tennhom": "bò nuôi thịt BCT 13-18 tháng",
    "danhsach": ["BoNuoiThitBCT"],
}
boVoBeoNho = {
    "tennhom": "bò vỗ béo nhỏ",
    "danhsach": ["BoVoBeoNho"],
}

boVoBeoTrung = {
    "tennhom": "bò vỗ béo trung",
    "danhsach": ["BoVoBeoTrung"],
}

boVoBeoLon = {
    "tennhom": "bò vỗ béo lớn",
    "danhsach": ["BoVoBeoLon"],
}

boChoPhoi = {"tennhom": "bò chờ phối", "danhsach": ["BoChoPhoi", "BoHauBiChoPhoi"]}
boMangThaiNho = {"tennhom": "bò mang thai 2-7 tháng", "danhsach": ["BoMangThaiNho"]}
boMangThaiLon = {"tennhom": "bò mang thai 8-9 tháng", "danhsach": ["BoMangThaiLon"]}
boMangThaiLonChoDe = {
    "tennhom": "bò mang thai 8-9 tháng",
    "danhsach": ["BoMangThaiLon", "BoChoDe"],
}
boMeNuoiConNho = {"tennhom": "bò mẹ nuôi con 0-1 tháng", "danhsach": ["BoMeNuoiConNho"]}
boMeNuoiConLon = {"tennhom": "bò mẹ nuôi con 2-4 tháng", "danhsach": ["BoMeNuoiConLon"]}
boDucGiong = {"tennhom": "bò đực giống", "danhsach": ["BoDucGiong"]}
tatCaPhanLoai = {"tennhom": "", "danhsach": []}

# Danh sách nhóm bò
nhomBoDucGiong = {"tennhom": "bò đực giống", "danhsach": ["BoDucGiong"]}
tatCaNhomBo = {"tennhom": "bò", "danhsach": ["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be"]}
nhombovabe = {"tennhom": "bò và bê", "danhsach": ["Bo","Be"]}
nhomBe = {"tennhom": "bê", "danhsach": ["Be"]}

# Gioi tinh
gioiTinhTatCa = {"tennhom": "", "danhsach": ["Đực", "Cái", "Không xác định", None, ""]}
gioiTinhCai = {"tennhom": "cái", "danhsach": ["Cái"]}
gioiTinhDuc = {"tennhom": "đực", "danhsach": ["Đực"]}

giongBo = [
    "Brahman",
    "Droughtmaster",
    "Angus",
    "Charolais",
    "BBB (Blan Blue Belgium)",
]

lanPhoi1 = {"min":1,"max":1}
lanPhoi2 = {"min":2,"max":2}
lanPhoi3 = {"min":3,"max":999}

"""
"""
# query.tongSoBo_saiLuaDe(client,db,"BoNhapTrai",startDate,endDate,ws)

# query.danhsachbe(client,db,"BoNhapTrai",startDate,endDate,ws,nhombo=nhombovabe)
# query.tinh_tongsobobe(client,db,"BoNhapTrai",startDate,endDate,ws)
# query.lichsuchuyenchuong(client,db,"ChuChuyenDan","MTC433")
# fileName = "testnhanh" + datetime.now().strftime("%Y%B%d%H%M%S.xlsx")
# wb.save(fileName)
current_moment = datetime.now()
result = test_result_collection.baocaothang.insert_one({
    "CreatedAt":current_moment,
    "NoiDung":"Test tạo một document",
    "Array":[]
})

newDocId = result.inserted_id

test_result_collection.baocaothang.update_one({"_id":newDocId},{"$push":{"Array":{"Content":"content","CreatedAt":datetime.now()}}})

print("updated successfully")


# Close mongo connection
# client.close()
