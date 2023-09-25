import test_dashboard.query as queryDashBoard
import test_phanquyen.test_phan_quyen as test_phan_quyen
import test_baocaothang.phoigiong as phoiGiong
import test_baocaothang.thongtindan as thongTinDan
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

# Connect to mongodb
client = MongoClient(constants.CONNECTION_STRING)
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
db = constants.DB

# Create workbook log
wb = Workbook()
ws = wb.active

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
danhsachnhombo = ["XuatBan", "Bo", "Be"]


# So luong bo cho phoi
thongTinDan.soBoChoPhoi(
    client, db, "BoNhapTrai", danhsachnhombo, "2023-09-01", "2023-09-20"
)

# So luong bo mang thai nho
thongTinDan.soBoMangThaiNho(
    client, db, "BoNhapTrai", danhsachnhombo, "2023-09-01", "2023-09-20"
)

# So luong bo mang thai lon cho de
thongTinDan.soBoMangThaiLonChoDe(
    client, db, "BoNhapTrai", danhsachnhombo, "2023-09-01", "2023-09-20"
)

# So luong bo nuoi con nho
thongTinDan.soBoNuoiConNho(
    client, db, "BoNhapTrai", danhsachnhombo, "2023-09-01", "2023-09-20"
)

# So luong bo nuoi con lon
thongTinDan.soBoNuoiConLon(
    client, db, "BoNhapTrai", danhsachnhombo, "2023-09-01", "2023-09-20"
)
"""

# Trong luong binh quan be cai cai sua
thongTinDan.trongLuongBinhQuan_beCaiCaiSua(client, db, "BoNhapTrai")

# Trong luong binh quan be duc cai sua
thongTinDan.trongLuongBinhQuan_beDucCaiSua(client, db, "BoNhapTrai")

# Tong so be cai cai sua
thongTinDan.tongSo_beCaiCaiSua(client, db, "BoNhapTrai")

# Tong so be duc cai sua
thongTinDan.tongSo_beDucCaiSua(client, db, "BoNhapTrai")

# 10	Tổng số bê cái hậu bị 9- 12 tháng
thongTinDan.tongSo_beCaiHauBi(client, db, "BoNhapTrai")

# 11	Tổng số bê đực hậu bị 9- 12 tháng
thongTinDan.tongSo_beDucHauBi(client, db, "BoNhapTrai")

# 12	Tổng số bê đực nuôi thịt BCT bị 9- 12 tháng
# 13	Tổng số bê cái nuôi thịt BCT bị 9- 12 tháng
# 14	Tổng số bò cái hậu bị BCT 13-18 tháng
# 15	Tổng số bò đực hậu bị BCT 13-18 tháng
# 16	Tổng số bò đực nuôi thịt BCT 13-18 tháng
# 17	Tổng số bò cái nuôi thịt BCT 13-18 tháng
# 18	Tổng số bò vỗ béo nhỏ
# 19	Tăng trọng bình quân của BVB nhỏ
# 20	Tổng số bò vỗ béo trung
# 21	Tăng trọng bình quân của BVB trung
# 22	Tổng số bò vỗ béo lớn
# 23	Tăng trọng bình quân của BVB lớn
# 24	Tổng số bò sinh sản nhập trại
# 25	Tổng số bê nhập trại
# 26	Tổng số bê sinh ra
# 27	Tổng số bê chết
# 28	Tổng số bò giống xuất bán
# 29	Tổng số bò vỗ béo xuất bán
# 30	Tổng số bê bệnh đang chờ thanh lý
# 31	Tổng số bò bệnh đang chờ thanh lý


# Close mongo connection
client.close()
