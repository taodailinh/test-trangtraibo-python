from datetime import datetime
import test_dashboard.query as queryDashBoard
import test_phanquyen.test_phan_quyen as test_phan_quyen
import test_baocaothang.phoigiong as phoiGiong
import test_baocaothang.thongtindan as thongTinDan
import test_baocaothang.thuy as thuY
import test_thongtindan.test_nhap_be as nhapBe
import test_thongtindan.xoabo as xoaBo
import constants
from pymongo import MongoClient
import time
import os
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


# Danh sách nhóm bò
boDucGiong = {"tennhom": "bò đực giống", "danhsach": ["BoDucGiong"]}
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

"""

# Print current path
script_path = os.path.dirname(os.path.abspath(__file__))
print("Current path")
print(script_path)

# Nhập bê
nhapBe.nhapbe(client, db, page, user, password)
time.sleep(10)
# Xóa bê vừa nhập
xoaBo.xoaBo(client, db, "BoNhapTrai", "F202300609")

# Test phân quyền
test_phan_quyen.testPhanQuyenUser("admin", "admintest", page)

phoiGiong.printAllKetQuaKhamThai(client, db, "ThongTinKhamThai")

print("----")

phoiGiong.printAllKetQuaKhamThaiInDateRange(
    client, db, "KhamThai", "2023-09-01", "2023-09-18"
)

phoiGiong.soLuongBoKham(client, db, "ThongTinKhamThai", "2023-08-01", "2023-09-18")

# Test query số bò phối có số lần phối
phoiGiong.soLuongBoKhamPhoiLan1(
    client, db, "ThongTinPhoiGiong", "2023-09-01", "2023-09-18"
)

# Danh sach bo it hon 100 ngay tuoi trong cac nhom bo
"""

# So luong bo cho phoi
thongTinDan.tongSoBo(
    client,
    db,
    "BoNhapTrai",
    startDate,
    endDate,
    ws,
    boChoPhoi,
)


# So luong bo mang thai nho
thongTinDan.tongSoBo(
    client,
    db,
    "BoNhapTrai",
    startDate,
    endDate,
    ws,
    boMangThaiNho,
)

# So luong bo mang thai lon cho de
thongTinDan.tongSoBo(
    client,
    db,
    "BoNhapTrai",
    startDate,
    endDate,
    ws,
    boMangThaiLonChoDe,
)

# So luong bo nuoi con nho
thongTinDan.tongSoBo(
    client,
    db,
    "BoNhapTrai",
    startDate,
    endDate,
    ws,
    boMeNuoiConNho,
)

# So luong bo nuoi con lon
thongTinDan.tongSoBo(
    client,
    db,
    "BoNhapTrai",
    startDate,
    endDate,
    ws,
    boMeNuoiConLon,
)

# Trong luong binh quan be cai cai sua
thongTinDan.trongLuongBinhQuan_beCaiCaiSua(client, db, "BoNhapTrai")

# Trong luong binh quan be duc cai sua
thongTinDan.trongLuongBinhQuan_beDucCaiSua(client, db, "BoNhapTrai")

# Tong so be cai cai sua
thongTinDan.tongSoBo(
    client, db, "BoNhapTrai", startDate, endDate, ws, beCaiSua, gioiTinhCai
)

# Tong so be duc cai sua
thongTinDan.tongSoBo(
    client, db, "BoNhapTrai", startDate, endDate, ws, beCaiSua, gioiTinhDuc
)

# 10	Tổng số bê cái hậu bị 9- 12 tháng
thongTinDan.tongSoBo(
    client, db, "BoNhapTrai", startDate, endDate, ws, boHauBi, gioiTinhCai
)

# 11	Tổng số bê đực hậu bị 9- 12 tháng
thongTinDan.tongSoBo(
    client, db, "BoNhapTrai", startDate, endDate, ws, boHauBi, gioiTinhDuc
)

# 12	Tổng số bê đực nuôi thịt BCT bị 9- 12
thongTinDan.tongSoBo(
    client, db, "BoNhapTrai", startDate, endDate, ws, boNuoiThitBCT9_12, gioiTinhDuc
)


# 13	Tổng số bê cái nuôi thịt BCT bị 9- 12 tháng
thongTinDan.tongSoBo(
    client, db, "BoNhapTrai", startDate, endDate, ws, boNuoiThitBCT9_12, gioiTinhCai
)

# 14	Tổng số bò cái hậu bị BCT 13-18 tháng
thongTinDan.tongSoBo(
    client, db, "BoNhapTrai", startDate, endDate, ws, boHauBiChoPhoi, gioiTinhCai
)
# 15	Tổng số bò đực hậu bị BCT 13-18 tháng
thongTinDan.tongSo_boDucHauBi_13_18(client, db, "BoNhapTrai")

# 16	Tổng số bò đực nuôi thịt BCT 13-18 tháng
# 17	Tổng số bò cái nuôi thịt BCT 13-18 tháng
# 18	Tổng số bò vỗ béo nhỏ
thongTinDan.tongSo_boVoBeoNho(client, db, "BoNhapTrai")

# 19	Tăng trọng bình quân của BVB nhỏ
# 20	Tổng số bò vỗ béo trung
# 21	Tăng trọng bình quân của BVB trung
# 22	Tổng số bò vỗ béo lớn
# 23	Tăng trọng bình quân của BVB lớn
# 24	Tổng số bò sinh sản nhập trại
thongTinDan.tongSo_nhapTrai_boSinhSan(client, db, "BoNhapTrai", startDate, endDate)

# 25	Tổng số bê nhập trại
thongTinDan.tongSo_nhapTrai_be(client, db, "BoNhapTrai", startDate, endDate)


# 26	Tổng số bê sinh ra
thongTinDan.tongSo_beSinh(client, db, "BoNhapTrai", startDate, endDate)

# 27	Tổng số bê chết
thongTinDan.tongSo_chet_be(client, db, "BoNhapTrai", startDate, endDate)

# 28	Tổng số bò giống xuất bán
# 29	Tổng số bò vỗ béo xuất bán
# 30	Tổng số bê bệnh đang chờ thanh lý
# 31	Tổng số bò bệnh đang chờ thanh lý


# TEST BAO CAO THANG THU Y
print("-- Test bao cao thu y --")
# 1,1	Tổng số bò đã điều trị khỏi bệnh
thuY.tongSo_boKhoiBenh(client, db, "DieuTriBoBenh", startDate, endDate)

# 1,2	Tổng số bò đã điều trị  (Chết):
thuY.tongSo_boChetCoDieuTri(client, db, "DieuTriBoBenh", startDate, endDate)

# 1,3	Tổng số bò mắc bệnh đã đề nghị bán thanh lý
thuY.tongSo_boDaDeXuatThanhLy(client, db, "ThanhLyBo", startDate, endDate, ws)

# Tổng số bò vỗ béo đã và đang điều trị
# 2,1	Tổng số bò vỗ béo nhỏ đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boVoBeoNho(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)

# 2,2	Tổng số bò vỗ béo trung đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boVoBeoTrung(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)

# 2,3	Tổng số bò vỗ béo lớn đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boVoBeoLon(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)

# 2,5	Tổng số bò vỗ béo đã điều trị Khỏi bệnh
thuY.tongSo_boKhoiBenh_boVoBeo(client, db, "DieuTriBoBenh", startDate, endDate, ws)

# 2,6	Tổng số bò vỗ béo đã điều trị không khỏi bệnh
thuY.tongSo_boKhongKhoiBenh_boVoBeo(client, db, "DieuTriBoBenh", startDate, endDate, ws)

# 2,7	Tổng số bò vỗ béo mắc bệnh đã đề nghị bán thanh lý
thuY.tongSo_boDaDeXuatThanhLy_boVoBeo(client, db, "ThanhLyBo", startDate, endDate, ws)

# Tổng số bò sinh sản đã và đang điều trị
# 3,1	Tổng số bò chờ phối đang điều trị
thuY.tongSo_boDaDangDieuTri_boChoPhoi(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)


# 3,2	Tổng số bò mang thai 2-7 tháng đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boMangThaiNho(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)
# 3,3	Tổng số bò mang thai 8-9 tháng đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boMangThaiLon(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)

# 3,4	Tổng số bò nuôi con 0-1 tháng đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boNuoiConNho(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)

# 3,5	Tổng số bò nuôi con ≥1-4 tháng đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boNuoiConLon(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)

# 3,6	Tổng số bò hậu bị  9-12 tháng đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boHauBi(client, db, "DieuTriBoBenh", startDate, endDate, ws)


# 3,7	Tổng số bò hậu bị  13-18 tháng đã và đang điều trị
thuY.tongSo_boDaDangDieuTri_boHauBiChoPhoi(
    client, db, "DieuTriBoBenh", startDate, endDate, ws
)

# 3,8	Tổng số bò thịt  13-18 tháng đã và đang điều trị
# Tổng số bê đã và đang điều trị
# 4,1	Tổng số bê từ 0-1 tháng đã và đang điều trị
# 4,2	Tổng số bê từ ≥ 1-4 tháng đã và đang điều trị
# 4,3	Tổng số bê từ cai sữa ≥ 4 tháng đến 8 tháng đã và đang điều trị
# 4,4	Tổng số bê đã điều trị khỏi bệnh
# 4,5	Tổng số bê đã điều trị không khỏi bệnh
# 4,6	Tổng số bê mắc bệnh đã đề nghị bán thanh lý
# Bệnh tật và tính thích nghi của từng giống bò


for bo in giongBo:
    # 5,1	Tổng số bê giống Brahman từ 0-1 tháng tuổi mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        beDuoi1thang,
    )
    # 5,2	Tổng số bê giống Brahman từ ≥ 1-4 tháng tuổi mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        beTheoMe,
    )

    # 5,3	Tổng số bê cái giống Brahman từ ≥4-8 tháng tuổi mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        beCaiSua,
        gioiTinhCai,
    )

    # 5,4	Tổng số bê đực giống Brahman từ ≥4-8 tháng tuổi mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        beCaiSua,
        gioiTinhDuc,
    )
    # 5,5	Tổng số bò cái giống Brahman từ 9-12 tháng tuổi mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        beCaiSua,
        gioiTinhCai,
    )
    # 5,6	Tổng số bò đực giống Brahman từ 9-12 tháng tuổi mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boNuoiThitBCT9_12,
        gioiTinhDuc,
    )

    # 5,7	Tổng số bò cái giống Brahman từ 13-18 tháng tuổi mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boHauBiChoPhoi,
        gioiTinhCai,
    )

    # 5,8	Tổng số bò đực giống Brahman từ 13-18 tháng tuổi mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boNuoiThitBCT13_18,
        gioiTinhCai,
    )

    # 5,9	Tổng số bò giống Brahman chờ phối mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boChoPhoi,
    )

    # 5,10	Tổng số bò giống Brahman mang thai 2-7 tháng mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boMangThaiNho,
    )

    # 5,11	Tổng số bò giống Brahman mang thai 8-9 tháng mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boMangThaiLon,
    )
    # 5,12	Tổng số bò giống Brahmannuôi con 0-1 tháng mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boMeNuoiConNho,
    )

    # 5,13	Tổng số bò giống Brahmannuôi con ≥ 2-4 tháng mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boMeNuoiConLon,
    )

    # 5,14	Tổng số bò đực vỗ béo nhỏ giống brahman mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boVoBeoNho,
        gioiTinhDuc,
    )

    # 5,15	Tổng số bò đực vỗ béo trung giống brahman mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boVoBeoTrung,
        gioiTinhDuc,
    )

    # 5,16	Tổng số bò đực vỗ béo lớn giống brahman mắc bệnh
    thuY.tongSo_boDaDangDieuTri_theoGiongBo(
        client,
        db,
        "DieuTriBoBenh",
        startDate,
        endDate,
        ws,
        bo,
        boVoBeoLon,
        gioiTinhDuc,
    )

# 5,17	Tổng số bê giống Drougth master từ 0-1 tháng tuổi mắc bệnh

# 5,18	Tổng số bê giống Drougth master từ ≥ 1-4 tháng tuổi mắc bệnh

# 5,19	Tổng số bê cái giống Drougth master từ ≥4-8 tháng tuổi mắc bệnh
# 5,20	Tổng số bê đực giống Drougth master từ ≥4-8 tháng tuổi mắc bệnh
# 5,21	Tổng số bò cái giống Drougth master từ 9-12 tháng tuổi mắc bệnh
# 5,22	Tổng số bò đực giống Drougth master từ 9-12 tháng tuổi mắc bệnh
# 5,23	Tổng số bò cái giống Drougth master từ 13-18 tháng tuổi mắc bệnh
# 5,24	Tổng số bò đực giống Drougth master từ 13-18 tháng tuổi mắc bệnh
# 5,25	Tổng số bò giống Drougth master chờ phối mắc bệnh
# 5,26	Tổng số bò giống Drougth master mang thai 2-7 tháng mắc bệnh
# 5,27	Tổng số bò giống Drougth master mang thai 8-9 tháng mắc bệnh
# 5,28	Tổng số bò giống Drougth master nuôi con 0-1 tháng mắc bệnh
# 5,29	Tổng số bò giống Drougth master nuôi con ≥ 2-4 tháng mắc bệnh
# 5,30	Tổng số bò đực vỗ béo nhỏ giống Drougth master mắc bệnh
# 5,31	Tổng số bò đực vỗ béo trung giống Drougth master mắc bệnh
# 5,32	Tổng số bò đực vỗ béo lớn giống Drougth master mắc bệnh
# 5,33	Tổng số bê giống Angus từ 0-1 tháng tuổi mắc bệnh
# 5,34	Tổng số bê giống Angus từ ≥ 1-4 tháng tuổi mắc bệnh
# 5,35	Tổng số bê cái giống Angus từ ≥4-8 tháng tuổi mắc bệnh
# 5,36	Tổng số bê đực giống Angus từ ≥4-8 tháng tuổi mắc bệnh
# 5,37	Tổng số bò cái giống Angus từ 9-12 tháng tuổi mắc bệnh
# 5,38	Tổng số bò đực giống Angus từ 9-12 tháng tuổi mắc bệnh
# 5,39	Tổng số bò cái giống Angus từ 13-18 tháng tuổi mắc bệnh
# 5,40	Tổng số bò đực giống Angus từ 13-18 tháng tuổi mắc bệnh
# 5,41	Tổng số bò giống Angus chờ phối mắc bệnh
# 5,42	Tổng số bò giống Angus mang thai 2-7 tháng mắc bệnh
# 5,43	Tổng số bò giống Angus mang thai 8-9 tháng mắc bệnh
# 5,44	Tổng số bò giống Angus nuôi con 0-1 tháng mắc bệnh
# 5,45	Tổng số bò giống Angus nuôi con ≥ 2-4 tháng mắc bệnh
# 5,46	Tổng số bò đực vỗ béo nhỏ giống Angus mắc bệnh
# 5,47	Tổng số bò đực vỗ béo trung giống Angus mắc bệnh
# 5,48	Tổng số bò đực vỗ béo lớn giống Angus mắc bệnh
# 5,49	Tổng số bê giống Charolaire từ 0-1 tháng tuổi mắc bệnh
# 5,50	Tổng số bê giống Charolaire từ ≥ 1-4 tháng tuổi mắc bệnh
# 5,51	Tổng số bê cái giống Charolaire từ ≥4-8 tháng tuổi mắc bệnh
# 5,52	Tổng số bê đực giống Charolaire từ ≥4-8 tháng tuổi mắc bệnh
# 5,53	Tổng số bò cái giống Charolaire từ 9-12 tháng tuổi mắc bệnh
# 5,54	Tổng số bò đực giống Charolaire từ 9-12 tháng tuổi mắc bệnh
# 5,55	Tổng số bò cái giống Charolaire từ 13-18 tháng tuổi mắc bệnh
# 5,56	Tổng số bò đực giống Charolaire từ 13-18 tháng tuổi mắc bệnh
# 5,57	Tổng số bò giốngCharolaire chờ phối mắc bệnh
# 5,58	Tổng số bò giống Charolaire mang thai 2-7 tháng mắc bệnh
# 5,59	Tổng số bò giống Charolaire mang thai 8-9 tháng mắc bệnh
# 5,60	Tổng số bò giống Charolaire nuôi con 0-1 tháng mắc bệnh
# 5,61	Tổng số bò giống Charolaire nuôi con ≥ 2-4 tháng mắc bệnh
# 5,62	Tổng số bò đực vỗ béo nhỏ giống Charolaire mắc bệnh
# 5,63	Tổng số bò đực vỗ béo trung giống Charolaire mắc bệnh
# 5,64	Tổng số bò đực vỗ béo lớn giống Charolaire mắc bệnh
# 5,65	Tổng số bê giống BBB (Blan Blue Belgium) từ 0-1 tháng tuổi mắc bệnh
# 5,66	Tổng số bê giống BBB (Blan Blue Belgium) từ ≥ 1-4 tháng tuổi mắc bệnh
# 5,67	Tổng số bê cái giống BBB (Blan Blue Belgium) từ ≥4-8 tháng tuổi mắc bệnh
# 5,68	Tổng số bê đực giống BBB (Blan Blue Belgium) từ ≥4-8 tháng tuổi mắc bệnh
# 5,69	Tổng số bò cái giống BBB (Blan Blue Belgium) từ 9-12 tháng tuổi mắc bệnh
# 5,70	Tổng số bò đực giống BBB (Blan Blue Belgium) từ 9-12 tháng tuổi mắc bệnh
# 5,71	Tổng số bò cái giống BBB (Blan Blue Belgium) từ 13-18 tháng tuổi mắc bệnh
# 5,72	Tổng số bò đực giống BBB (Blan Blue Belgium) từ 13-18 tháng tuổi mắc bệnh
# 5,73	Tổng số bò giống BBB (Blan Blue Belgium) chờ phối mắc bệnh
# 5,74	Tổng số bò giống BBB (Blan Blue Belgium) mang thai 2-7 tháng mắc bệnh
# 5,75	Tổng số bò giống BBB (Blan Blue Belgium) mang thai 8-9 tháng mắc bệnh
# 5,76	Tổng số bò giống BBB (Blan Blue Belgium) nuôi con 0-1 tháng mắc bệnh
# 5,77	Tổng số bò giống BBB (Blan Blue Belgium) nuôi con ≥ 2-4 tháng mắc bệnh
# 5,78	Tổng số bò đực vỗ béo nhỏ giống BBB (Blan Blue Belgium) mắc bệnh
# 5,79	Tổng số bò đực vỗ béo trung giống BBB (Blan Blue Belgium) mắc bệnh
# 5,80	Tổng số bò đực vỗ béo lớn giống BBB (Blan Blue Belgium) mắc bệnh

# Xuat thong tin dan
"""
"""
for i in range(1,10000):
    print("-- Lan xuat thu "+str(i)+"--")
    thongTinDan.exportThongTinDan(client, db, "BoNhapTrai")
fileName = "baocaothang_" + datetime.now().strftime("%Y%B%d%H%M%S.xlsx")
wb.save(fileName)

# Close mongo connection
client.close()
