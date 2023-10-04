from datetime import datetime
import test_dashboard.query as queryDashBoard
import test_phanquyen.test_phan_quyen as test_phan_quyen
import test_baocaothang.phoigiong as phoiGiong
import test_baocaothang.thongtindan as thongTinDan
import test_baocaothang.thuy as thuY
import constants
from pymongo import MongoClient
from openpyxl import Workbook

page = "https://test-trangtrai.aristqnu.com/"

user = "admin"
password = "admintest"

startDate = "2023-09-01"
endDate = "2023-09-30"

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

# 1	Tổng số bò đực giống đã được đề xuất thanh lý
phoiGiong.tongSoBoThanhLy_BoDucGiong(
    client, db, "ThanhLyBo", "thanh lý", startDate, endDate, ws, boDucGiong
)

# 2	Bò không đủ tiêu chuẩn xử lý sinh sản
phoiGiong.nghiepVu_tongSoBo(client,db,"KhamSinhSan","khám sinh sản không đạt tiêu chuẩn",startDate,endDate,ws,tatCaPhanLoai,"NgayKham","DatTieuChuanPhoi",False)

# 3	Tổng số bò được xử lý hormone sinh sản ngày 0
phoiGiong.tongSo_XLSS(client,db,"XuLySinhSan","xử lý sinh sản ngày 0",startDate,endDate,ws,"0")

# 4	Tổng số bò được xử lý hormone sinh sản 7
phoiGiong.tongSo_XLSS(client,db,"XuLySinhSan","xử lý sinh sản ngày 7",startDate,endDate,ws,"7")

# 5	Tổng số bò được xử lý hormone sinh sản 9
phoiGiong.tongSo_XLSS(client,db,"XuLySinhSan","xử lý sinh sản ngày 9",startDate,endDate,ws,"9")

# 6	Tổng số bò được xử lý hormone sinh sản 10
phoiGiong.tongSo_XLSS(client,db,"XuLySinhSan","xử lý sinh sản ngày 10",startDate,endDate,ws,"10")

# 7	Tổng số bò được gieo tinh nhân tạo từ bò lên giống tự nhiên (không xử lý sinh sản)
# phoiGiong.tongSo_phoiGiongTuNhien(client,db,"ThongTinPhoiGiong","phối giống tự nhiên",startDate,endDate,ws)

# phoiGiong.tongSo_phoiGiongTuNhien_ver2(client,db,"ThongTinPhoiGiong","phối giống tự nhiên",startDate,endDate,ws)

# phoiGiong.tongSo_phoiGiongTuNhien_ver3(client,db,"ThongTinPhoiGiong","phối giống tự nhiên",startDate,endDate,ws)

phoiGiong.tongSo_phoiGiongTuNhien_ver4(client,db,"ThongTinPhoiGiong","phối giống tự nhiên",startDate,endDate,ws)

phoiGiong.tongSo_phoiGiongSauXLSS(client,db,"ThongTinPhoiGiong","phối giống sau khi xlss",startDate,endDate,ws)


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

fileName = "baocaothang_" + datetime.now().strftime("%Y%B%d%H%M%S.xlsx")
wb.save(fileName)

# Close mongo connection
client.close()
