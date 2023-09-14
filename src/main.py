import test_dashboard.query as queryDashBoard
import test_phanquyen.test_phan_quyen as test_phan_quyen
import test_baocaothang
import test_thongtindan.test_nhap_be as nhapBe
import test_thongtindan.xoabo as xoaBo
import constants
from pymongo import MongoClient
import time
import os

page = "https://test-trangtrai.aristqnu.com/"

user = "admin"
password = "admintest"

client = MongoClient(constants.CONNECTION_STRING)
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
db = constants.DB

# Print current path
script_path = os.path.dirname(os.path.abspath(__file__))
print("Current path")
print(script_path)

"""
# Nhập bê
nhapBe.nhapbe(client, db, page, user, password)
time.sleep(10)
"""
# Xóa bê vừa nhập
xoaBo.xoaBo(client, db, "BoNhapTrai", "F202300609")

# Test phân quyền
test_phan_quyen.testPhanQuyenUser("admin", "admintest", page)
