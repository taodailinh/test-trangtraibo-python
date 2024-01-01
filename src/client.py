import constants
from pymongo import MongoClient


# Trại làm việc

client = MongoClient(constants.CONNECTION_STRING)
traibo_db = client[constants.DB]


class DB:
    def __init__(self, traibo_db, trai=0):
        self.bonhaptrai = traibo_db["BoNhapTrai" + constants.TENANTS[trai]]
        self.khamthai = traibo_db["KhamThai" + constants.TENANTS[trai]]
        self.thanhly = traibo_db["ThanhLyBo" + constants.TENANTS[trai]]
        self.xlss = traibo_db["XuLySinhSan" + constants.TENANTS[trai]]
        self.phoigiong = traibo_db["ThongTinPhoiGiong" + constants.TENANTS[trai]]
        self.dieutri = traibo_db["DieuTriBoBenh" + constants.TENANTS[trai]]
        self.vaccine = traibo_db["ThongTinTiemVaccine" + constants.TENANTS[trai]]
        self.lieutrinhvaccine = traibo_db["LieuTrinhVaccine" + constants.TENANTS[trai]]
        self.canbo = traibo_db["CanBo" + constants.TENANTS[trai]]

    def phoigiong_aggregate(self, pipeline):
        return self.phoigiong.aggregate(pipeline)

    def khamthai_aggregate(self, pipeline):
        return self.khamthai.aggregate(pipeline)

    def dieutri_aggregate(self, pipeline):
        return self.dieutri.aggregate(pipeline)

    def bonhaptrai_aggregate(self, pipeline):
        return self.bonhaptrai.aggregate(pipeline)

    def canbo_aggregate(self, pipeline):
        return self.canbo.aggregate(pipeline)
    
    def vaccine_aggregate(self,pipeline):
        return self.vaccine.aggregate(pipeline)

    def xlss_aggregate(self,pipeline):
        return self.xlss.aggregate(pipeline)

    # Đổi trại
    def changeFarm(self, trai):
        self.__init__(traibo_db, trai)
        print(self.bonhaptrai)


db = DB(traibo_db)

# Trại bò db

# Bò nhập trại

# Điều trị bò bệnh


test_result_db = client[constants.TEST_RESULT_DB]


class DB_TEST_RESULT:
    def __init__(self, trai=0):
        self.baocaothang = test_result_db["BaoCaoThang" + constants.TENANTS[trai]]

    # Đổi trại
    def changeFarm(self, trai):
        self.__init__(trai)
        # self.bonhaptrai = traibo_db["BoNhapTrai"+constants.TENANTS[trai]]
        print(self.baocaothang)
        # self.dieutribobenh = traibo_db["DieuTriBoBenh"+constants.TENANTS[trai]]


# Test result db

test_result_collection = DB_TEST_RESULT()


def changeFarm(trai):
    db.changeFarm(trai)
    test_result_collection.changeFarm(trai)