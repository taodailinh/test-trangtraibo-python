
from openpyxl import Workbook
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time
from datetime import datetime
from selenium.webdriver import Keys, ActionChains
from pymongo import MongoClient
import test_dashboard.query as querydashboard
import constants


os.environ["PATH"] += "C:/Users/taoda/test/selenium/env"

client = MongoClient(constants.CONNECTION_STRING)
db = constants.DB
startDate = "2023-09-22"
endDate = "2023-10-22"

tatCaNhomBo = {
    "tennhom": "bò",
    "danhsach": ["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be", None],
}

tatCaNhomBoSong = {
    "tennhom": "bò tổng đàn",
    "danhsach": ["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be"],
}

gioiTinhTatCa = {
    "tennhom": "",
    "danhsach": ["Đực", "Cái", "Không xác định", None, ""],
}

tatCaPhanLoai = {"tennhom":"","danhsach":["BoMoiPhoi",
        "BoMangThaiNho",
        "BoChoPhoi",
        "BoXuLySinhSan",
        "BoMeNuoiConNho",
        "BoChoDe",
        "BoMangThaiLon",
        "BoMeNuoiConLon",
        "BoVoBeoNho",
        "BoHauBiChoPhoi",
        "BoNuoiThitBCT",
        "BoHauBi",
        "BoNuoiThitBCT8_12",
        "BeCaiSua",
        "BeTheoMe",
        "BeSinh",
        "BoCachLy",
        "",
]}

wb = Workbook()
ws = wb.active

# Tìm 1 con bò bằng sô tai và in số chip
# for i in boNhapTrai.find({"SoTai": "F040923"}):
#     print(i["SoChip"])


# querydashboard.tongdanbo(client,db,"BoNhapTrai",tatCaNhomBoSong,tatCaPhanLoai,ws)
querydashboard.calArea(client,db)

danhsachhangmuccongviec = [
    " Trồng cỏ pangola",
    "Ban đất thủ công",
    "Ban, lấp mặt bằng cục bộ",
    "Bón bổ sung bắp (phân bò)",
    "Bón bổ sung bắp (phân npk)",
    "Bón lót hố cây lâm nghiệp",
    "Bón lót phân NPK",
    "Bón lót phân bò",
    "Bón phân bổ sung",
    "Bón phân thúc - Lần 1",
    "Bón phân thúc - Lần 2",
    "Bón phân thúc - Lần 3",
    "Bón phân thúc bắp lần 1",
    "Bón phân vi sinh",
    "Bón thúc bắp - Lần 1",
    "Bón thúc bắp - Lần 2",
    "Bón thúc bắp - Lần 3",
    "Bốc bắp lên xe bằng tay",
    "Bốc cỏ mombasa lên xe bằng tay",
    "Bốc cỏ mulato lên xe bằng tay",
    "Bốc cỏ voi lên xe bằng tay",
    "Bốc giống lên xe chuyển qua campuchia",
    "Bốc rải trụ hàng rào",
    "Bốc vật tư nông nghiệp ra lô",
    "Bừa 18 chảo - Lần 3",
    "Bừa 18 chảo lần 1",
    "Bừa 18 chảo lần 2",
    "Chi phí phay đất",
    "Chi phí trồng mới bằng máy",
    "Chặt bắp",
    "Chặt chồi cành",
    "Chặt cây le cắm tiêu",
    "Chặt cỏ giống, vận chuyển ra lô",
    "Chặt cỏ mombasa",
    "Chặt cỏ mulato",
    "Chặt cỏ voi",
    "Chặt dọn cao su bị đỗ gãy",
    "Chặt hom giống xuất đi Campuchia",
    "Chặt hom, rãi giống, trồng mới",
    "Chặt xử lý sâu bệnh (phát sinh phải làm tờ trình phê duyệt)",
    "Chẻ tiêu",
    "Chống úng",
    "Cào, gom, bốc cỏ",
    "Cày 5 chảo - Lần 1",
    "Cày 5 chảo - Lần 2",
    "Cày 5 chảo - Lần 3",
    "Cày 8 chảo - Lần 1",
    "Cày 8 chảo - lần 2",
    "Cày 8 chảo - lần 3",
    "Cày rạch hàng",
    "Cày sâu bón phân (phát sinh phải làm tờ trình phê duyệt)",
    "Công theo xe trồng",
    "Cắm cờ thu hoạch",
    "Cắm cờ đường ống chính",
    "Cắm tiêu hàng rào xung điện",
    "Cắm tiêu hệ thống tưới",
    "Cắt rễ cỏ giống, vận chuyển về lô",
    "Cắt trụ bê tông",
    "Cắt, Gom, bốc đường ống nước cũ về Kho",
    "Dọn vật tư khu Xoài về Kho",
    "Dọn vệ sinh lô",
    "Dọn đốt gốc cao su",
    "Dọn đốt vườn điều",
    "Dời đường ống 114 của máy bơm",
    "FORLAND140 chở đất làm đường vành đai",
    "Gia cố trụ súng",
    "Gom bốc cỏ voi lên xe sau chặt giống",
    "Gom cỏ giống ra đầu lô",
    "Gom cỏ sau tề gốc",
    "Gom cỏ voi",
    "Gắp bắp lên xe",
    "Gắp cỏ mombasa lên xe",
    "Gắp cỏ mulato lên xe",
    "Gắp cỏ voi lên xe",
    "Hạt giống bắp",
    "Hỗ trợ Kho rơm",
    "Hỗ trợ làm hệ thống tưới",
    "Khoan hố",
    "Khoan, dựng, chèn và bốc rải trụ hàng rào",
    "Kiểm tra hệ thống tưới",
    "Komatsu05 múc đất làm đường vành đai",
    "Làm bồn trồng cây lâm nghiệp",
    "Làm cỏ dại",
    "Làm hàng rào",
    "Làm hàng rào chống gia súc",
    "Làm trụ súng tưới",
    "Làm đường ống nước",
    "Lấp lại hố khoan",
    "Lấp đường ống nước 168",
    "Lấy nước phun thuốc",
    "Lắp đặt hệ thống tưới",
    "Nhân công theo xe tung phân",
    "Nhặt đá, rễ - Lần 1",
    "Nhặt đá, rễ - Lần 2",
    "Phay đất",
    "Phay đất - Lần 2",
    "Phun thuốc bón lá",
    "Phun thuốc bón lá - Lần 2",
    "Phun thuốc sâu bênh",
    "Phun thuốc sâu bệnh bắp",
    "Phun thuốc sâu bệnh bắp cơ giới",
    "Phun thuốc sâu bệnh bắp lần 2",
    "Phun thuốc sâu bệnh bắp lần 3",
    "Phun thuốc tiền nảy mầm",
    "Phun thuốc tiền nảy mầm (sau thu hoạch)",
    "Phun thuốc tiền nảy mầm lần 1",
    "Phun thuốc tiền nảy mầm lần 2",
    "Phun thuốc xử lý sâu bệnh/lần (6-7 lần/vụ)",
    "Phát cỏ bờ lô, dọn hộp thủy",
    "Phát cỏ dại",
    "Phát cỏ hàng 3 cao su ,vành đai",
    "Phát cỏ hàng cây ",
    "Phát cỏ thu hoạch",
    "Phát cỏ trong hàng cao su",
    "Phát cỏ tề gốc",
    "Phát lá cỏ lấy giống",
    "Phóng, cắm tiêu",
    "Phụn thuốc sâu bệnh cây lâm nghiệp",
    "Ra hom",
    "Rải lấp hạt giống (thủ công)",
    "San lấp hạt bắp",
    "San lấp mặt bằng cục bộ",
    "Sửa chữa hàng rào chống gia súc",
    "Sửa máy bơm",
    "Sửa trụ súng tưới",
    "Sửa trục đường lô",
    "Sửa đường ống nước 114",
    "Sửa đường ống nước 140",
    "Sửa đường ống nước 168",
    "Sửa đường ống nước 220",
    "Sửa ống nước trạm bơm",
    "Thu gom cành CS to, đốt cành CS nhỏ",
    "Thu gom, dọn đá sau thu hoạch",
    "Thu hoạch cơ giới",
    "Thu hoạch thủ công",
    "Thu hồi đường ống",
    "Thuốc Ridomil Gold",
    "Thuốc Selecron",
    "Thuốc Shepatin",
    "Tháo gỡ kẽm hàng rào",
    "Tháo hàng rào",
    "Tháo thép hộp hàng rào ",
    "Tháo, đan kẽm hàng rào",
    "Trung chuyển",
    "Trồng Cỏ - Hạt",
    "Trồng bắp",
    "Trồng bắp cơ giới",
    "Trồng bắp thủ công",
    "Trồng cây lâm nghiệp",
    "Trồng dặm",
    "Trồng dặm bắp",
    "Trồng dặm cây lâm nghiệp",
    "Trực bảo vệ khu Xoài",
    "Tung phân bò",
    "Tung phân hóa học",
    "Tung vôi",
    "Tách bụi Cỏ, bốc rãi, đào hô, trồng Cỏ",
    "Tăng ca bốc giống lên xe chuyển qua campuchia",
    "Tăng cường BCT1",
    "Tăng cường NTC1",
    "Tưới cây lâm nghiệp",
    "Tưới nước",
    "Tưới nước/ lần",
    "Tỉa cành cao su",
    "Vận chuyển bắp",
    "Vận chuyển bắp khu B",
    "Vận chuyển cỏ mombasa",
    "Vận chuyển cỏ mombasa Khu B",
    "Vận chuyển cỏ mulato",
    "Vận chuyển cỏ mulato Khu B",
    "Vận chuyển cỏ voi",
    "Vận chuyển vật tư",
    "Xe chở cỏ thu hoạch",
    "Xe thu hoạch băm cỏ",
    "Xe thu hoạch cắt cỏ",
    "Xe thu hoạch gắp cỏ",
    "Xúc, xới cát",
    "Đào chôn đá",
    "Đào cỏ gốc trồng dặm",
    "Đào hố ",
    "Đào hố trụ hàng rào",
    "Đào đường ống nước",
    "Đốt dọn lô"
  ]


danhsachhangmuccongviec_bonphanvoco = [
    "Bón bổ sung bắp (phân bò)",
    "Bón bổ sung bắp (phân npk)",
    "Bón lót hố cây lâm nghiệp",
    "Bón lót phân NPK",
    "Bón phân bổ sung",
    "Bón phân thúc - Lần 1",
    "Bón phân thúc - Lần 2",
    "Bón phân thúc - Lần 3",
    "Bón phân thúc bắp lần 1",
    "Bón thúc bắp - Lần 1",
    "Bón thúc bắp - Lần 2",
    "Bón thúc bắp - Lần 3",
    "Tung phân hóa học",]

danhsachhangmuccongviec_tegoc = [
    "Phát cỏ tề gốc",
  ]

tatcavattu = [
    "Acotrin 440 EC",
    "Amistar",
    "Antibactro",
    "Bình phun 20L",
    "Bó le",
    "Búa",
    "Co 114",
    "Co 140",
    "Cruiser plus",
    "Cuốc xạc",
    "Cào 8 răng",
    "Cát",
    "Cây dổi",
    "Cây giá tỵ",
    "Dây cước phóng tiêu",
    "Dây nilon đen",
    "Dây rút",
    "Dầu Diezel",
    "Fafix",
    "Fetri 35.11.11",
    "GA3",
    "Giảm 140/114",
    "Giảm 168/140",
    "Giống cỏ pangola",
    "Hạt giống bắp",
    "Hạt giống cỏ Mombasa",
    "Hạt giống cỏ Mulato",
    "Hạt giống cỏ Ruzi",
    "Keo dán ống",
    "Kéo",
    "Lân nung chảy",
    "Lơi 114",
    "Lơi 140",
    "Lưới lan",
    "Lưỡi cắt hợp kim",
    "Lưỡi dao máy phát cỏ",
    "Mặt bít 114",
    "Nhớt 2 thì",
    "Nối 114",
    "Nối 140",
    "Nối 168",
    "Phân Kali",
    "Phân NPK",
    "Phân NPK",
    "Phân Urê",
    "Phân bò",
    "Phân bò đóng bao",
    "Prevathon",
    "Reasgant 3.6 EC",
    "Rựa",
    "Thuốc  Amistar top 325SC",
    "Thuốc Antibatro + +",
    "Thuốc Ridomil Gold",
    "Thuốc Selecron 500EC",
    "Thuốc Shepatin 90EC",
    "Thuốc Virtako 40 WG",
    "Thuốc Wofatac 350EC",
    "Thuốc cỏ Atrazine",
    "Thuốc kiến Regant",
    "Tê 114",
    "Tê 140/114",
    "Tê 140/114",
    "Tê 168/114",
    "Vôi",
    "Xantocin 40 PW",
    "Xe rùa",
    "Xăng",
    "Xăng pha nhớt 2 thì",
    "Xăng roda máy",
    "Xẻng",
    "Điện năng",
    "Ống 114",
    "Ống 140"
  ]

phanvoco = [
    "Lân nung chảy",
    "Phân Kali",
    "Phân NPK",
    "Phân NPK",
    "Phân Urê",
 ]


# for hangmuc in danhsachhangmuccongviec:
#     querydashboard.dientichco_theohangmuccongviec(client,db,hangmuc,startDate,endDate,collection="ChiTietCongViecDongCo")

# Tổng diện tích các lô cỏ thực hiện tưới nước
querydashboard.dientichco_theohangmuccongviec(client,db,"Tưới nước",startDate,endDate,collection="ChiTietCongViecDongCo")


# querydashboard.dientichco_bonphanvoco(client,db,danhsachhangmuccongviec_bonphanvoco,startDate,endDate,collection="ChiTietCongViecDongCo")

querydashboard.dientichco_tegoc(client,db,danhsachhangmuccongviec_tegoc,startDate,endDate,collection="ChiTietCongViecDongCo")

querydashboard.tongkhoiluong_phanvoco(client,db,phanvoco,startDate,endDate,collection="ChiTietCongViecDongCo")

"""
# Add this to keep webdriver stay running
options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)

driver = webdriver.Chrome(options=options)

# maximize the window
driver.maximize_window()

driver.get("https://test-trangtraibo.aristqnu.com/")

driver.implicitly_wait(10)

username = driver.find_element(By.ID, "Input_UserName")

username.send_keys("admin")


password = driver.find_element(By.ID, "password-field")

password.send_keys("admintest")

form = driver.find_element(By.CLASS_NAME, "signin-form")
form.submit()

driver.implicitly_wait(20)





time.sleep(10)


time.sleep(10)
"""

fileName = "test_dashboard" + datetime.now().strftime("%Y%B%d%H%M%S.xlsx")
wb.save(fileName)
# driver.quit()
