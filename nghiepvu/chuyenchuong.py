from pymongo import MongoClient
from datetime import datetime, timedelta
import time
from openpyxl import Workbook

from client import db, test_result_collection, changeFarm


def capnhatochuonghientai(id):
    startTime=time.time()
    
    endTime=time.time()
    print("Tong thoi gian: "+str(endTime-startTime))