from datetime import datetime
import test_baocaothang.vaccine as vaccine
import constants
from pymongo import MongoClient
from openpyxl import Workbook

page = "https://test-trangtrai.aristqnu.com/"

user = "admin"
password = "admintest"

startDate = "2023-09-01"
endDate = "2023-09-30"
workingDate = "2023-10-17"

# Connect to mongodb
client = MongoClient(constants.CONNECTION_STRING)
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
db = constants.DB

# Create workbook log
wb = Workbook()
ws = wb.active
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
nhomBe = {"tennhom": "bê", "danhsach": ["BoChuyenVoBeo", "Be"]}

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

# lanPhoi1 = {"min":1,"max":1}
# lanPhoi2 = {"min":2,"max":2}
# lanPhoi3 = {"min":3,"max":999}

# Tổng số bò tiêm vaccine 
nhomVaccine = {
    "THT":{"ma":"THT","_id":"e5667d05-eea3-4586-a002-fc111d2792c3"},
    "UKT":{"ma":"UKT","_id":"46fb8541-0597-42e6-9955-01beb535b64f"},
    "VDNC":{"ma":"VDNC","_id":"8a869cb0-1ecd-49d1-9ef6-3f6353a73a84"},
    "LMLM":{"ma":"LMLM","_id":"d903508e-645d-4392-9eb1-ae763e2f68fc"},
    "LEPTOSPIRIOSIS":{"ma":"LEPTOSPIRIOSIS","_id":"1f598dfe-e686-49a4-aaf4-4471643375f4"},
    "NOINGOAIKYSINHTRUNG":{"ma":"NOINGOAIKYSINHTRUNG","_id":"ed980a55-d38f-46fa-a1c3-6f571d61fbb1"},
    "VIEMRONVIEMPHOI":{"ma":"VIEMRONVIEMPHOI","_id":"a62892df-ab82-4a6e-b4f0-2874d16c15b9"},
    "CAUTRUNG":{"ma":"CAUTRUNG","_id":"e3c74a1a-1fc7-4643-80fd-b594563b07cf"},
    "TIEUCHAY":{"ma":"TIEUCHAY","_id":"1482b4f5-805f-458d-b5a1-70fb03cddf00"},
    "UKT 7in1":{"ma":"UKT 7in1","_id":"b19b3a4a-c3bd-4790-9a3a-6e33d33b65f5"}
}

vaccinethongke = ["UKT",
# "VDNC","LMLM","LEPTOSPIRIOSIS","NOINGOAIKYSINHTRUNG","VIEMRONVIEMPHOI","CAUTRUNG","TIEUCHAY","UKT 7in1"
]


for loaivaccine in vaccinethongke:
    # vaccine.tongSo_boDuocTiemVaccine(client,db,"ThongTinTiemVaccine",startDate,endDate,ws,nhomVaccine[loaivaccine])
    vaccine.tongSo_boDuDieuKienTiem(workingDate,nhomVaccine[loaivaccine])

vaccine.tongSo_boDuDieuKienTiem_THT(workingDate,nhomVaccine["THT"])


fileName = "baocaothang_vaccine" + datetime.now().strftime("%Y%B%d%H%M%S.xlsx")
wb.save(fileName)

# Close mongo connection
client.close()
