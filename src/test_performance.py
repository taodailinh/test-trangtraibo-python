from datetime import datetime
import test_performance.test_hieu_nang as perform
import constants
from pymongo import MongoClient
import time
import os
from openpyxl import Workbook
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from threading import Thread, current_thread

os.environ["PATH"] += "C:/Users/taoda/test/selenium/env"

# Connect to mongodb
client = MongoClient(constants.CONNECTION_STRING)
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
db = constants.DB


# Add this to keep webdriver stay running
options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)

# maximize the window

drivers =[]
for i in range(1,2):
    driver_i = webdriver.Chrome(options=options)
    driver_i.maximize_window()
    drivers.append(driver_i)

driverPhoiGiong = webdriver.Chrome(options=options)
driverPhoiGiong.maximize_window()

driverTaoPhieuKSS = webdriver.Chrome(options=options)
driverTaoPhieuKSS.maximize_window()


page = "https://dev-trangtraibo.aristqnu.com/"
for driver in drivers:
    driver.get(page)
    user = "admin"
    pw = "admintest"
    wait = WebDriverWait(driver,10)
    driver.get(page+"account/login?redirectUri=/")
    wait.until(EC.presence_of_element_located((By.ID, "Input_UserName"))).send_keys(str(user))
    # username = driver.find_element(By.ID, "Input_UserName")
    # username.send_keys(str(user))
    wait.until(EC.presence_of_element_located((By.ID, "password-field"))).send_keys(str(pw))
    # password = driver.find_element(By.ID, "password-field")
    # password.send_keys(str(pw))
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Đăng nhập')]"))).click()
    # form = driver.find_element(By.CLASS_NAME, "signin-form")
    # form.submit()

def xuatthongtindan(client,drivers,page):
    perform.testXuatThongTinDan(client,drivers,page)

def taophieuphoigiong(client,driver, page):
    driver.get(page)
    user = "admin"
    pw = "admintest"
    wait = WebDriverWait(driver,10)
    driver.get(page+"account/login?redirectUri=/")
    wait.until(EC.presence_of_element_located((By.ID, "Input_UserName"))).send_keys(str(user))
    wait.until(EC.presence_of_element_located((By.ID, "password-field"))).send_keys(str(pw))
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Đăng nhập')]"))).click()

    #bam nut di truyền nhân giống
    driver.get(page+"quanlydan/danhsachdan")
    # bấm nút khám sinh sản
    wait = WebDriverWait(driver,200)
    time.sleep(5)
    wait.until(EC.invisibility_of_element((By.XPATH,"/html/body/div/app/div/div/div/div[5]/div[2]/div[1]")))
    ditruyen = wait.until(EC.element_to_be_clickable((By.XPATH,"//button[text()='Di truyền nhân giống']")))
    ditruyen.click()
    kss = wait.until(EC.element_to_be_clickable((By.XPATH,"//li[text()='Khám sinh sản']")))
    kss.click()
    time.sleep(5)
    # chọn tất cả 25 con
    wait.until(EC.invisibility_of_element((By.XPATH,"/html/body/div/app/div/div/div/div[5]/div[2]/div[1]")))
    time.sleep(1)
    checkall = wait.until(EC.element_to_be_clickable((By.XPATH,"/html/body/div[1]/app/div/div/div/div[5]/div/div[2]/div/table/thead/tr/th[1]/div[1]/div")))
    checkall.click()
    # bấm nút tạo phiếu
    taophieu = wait.until(EC.element_to_be_clickable((By.XPATH,"//button[text()='Tạo phiếu công việc']")))
    taophieu.click()
    # điền thông tin form

    # bấm nút tạo phiếu

def vaotrangphoigiong(client,driver,page):
    driver.get(page)
    user = "admin"
    pw = "admintest"
    wait = WebDriverWait(driver,10)
    driver.get(page+"account/login?redirectUri=/")
    wait.until(EC.presence_of_element_located((By.ID, "Input_UserName"))).send_keys(str(user))
    # username = driver.find_element(By.ID, "Input_UserName")
    # username.send_keys(str(user))
    wait.until(EC.presence_of_element_located((By.ID, "password-field"))).send_keys(str(pw))
    # password = driver.find_element(By.ID, "password-field")
    # password.send_keys(str(pw))
    wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Đăng nhập')]"))).click()
    # form = driver.find_element(By.CLASS_NAME, "signin-form")
    # form.submit()
# for driver in drivers:
#     driver.quit()
    time.sleep(5)
    driver.get(page+"phoigiong/khamphoigiong")
    # Tìm bò phối giống
    # Phối giống
    # Lưu phối giống
    # Lưu lại sô tai
    # Xóa phối giống
    # Xóa thông tin phối giống của bò
    # Cập nhật giai đoạn bò

t1 = Thread(target = xuatthongtindan, args =(client,drivers,page ))
t2 = Thread(target = vaotrangphoigiong, args =(client,driverPhoiGiong,page ))
t3 = Thread(target = taophieuphoigiong, args =(client,driverTaoPhieuKSS,page ))


# t1.start()
t2.start()
t3.start()
# t1.join()
t2.join()
t3.join()



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


# Print current path
# script_path = os.path.dirname(os.path.abspath(__file__))
# print("Current path")
# print(script_path)

# Nhập bê
time.sleep(5)
# Xóa bê vừa nhập

# Test xuất danh sách đàn
# for i in range(0,100):
#     print(i)

"""

"""
# fileName = "testhieunang" + datetime.now().strftime("%Y%B%d%H%M%S.xlsx")
# wb.save(fileName)

# Close mongo connection
client.close()
