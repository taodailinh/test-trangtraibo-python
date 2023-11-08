from datetime import datetime
import test_baocao_thuy.dieutritheobenh as dieutritheobenh
import constants
from pymongo import MongoClient
from openpyxl import Workbook

startDate = "2023-09-01"
endDate = "2023-09-30"


# Connect to mongodb
client = MongoClient(constants.CONNECTION_STRING)
# client = MongoClient("mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/")
db = constants.DB

dieutritheobenh.dangdieutri(client,db,"DieuTriBoBenh")
dieutritheobenh.khongdangdieutri(client,db,"BoNhapTrai")

client.close()