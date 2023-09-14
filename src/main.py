import test_dashboard.query as queryDashBoard
import test_phanquyen.test_phan_quyen as test_phan_quyen
import test_baocaothang
import constants
from pymongo import MongoClient

page = "https://test-trangtrai.aristqnu.com/"

client = MongoClient(constants.CONNECTION_STRING)
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
db = constants.DB

queryDashBoard.printAllNongTruong(client, db)
test_phan_quyen.testPhanQuyenUser("admin", "admintest", page)
