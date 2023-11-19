# CONNECTION_STRING = "mongodb://thagrico:Abc%40%23%24123321@45.119.84.161:27017/"
BASE = "https://dev-trangtraibo.aristqnu.com"
CONNECTION_STRING = "mongodb://admin:admintest@45.119.84.161:27017/"
TEST_RESULT_DB = "Linh_Test"
TEST_RESULT_COL_BAOCAOTHANG = "BaoCaoThang"
DB = "QuanLyTrangTrai_0811"
DB_COL_BONHAPTRAI = "BoNhapTrai"
DB_COL_TIEMVACCINE = "ThongTinTiemVaccine"

START_DATE = "2023-10-01"
END_DATE = "2023-10-31"

TENANTS = ["","_1","_2","_3","_4"]

giaiDoanBoVoBeo = ["BoVoBeoNho", "BoVoBeoTrung", "BoVoBeoLon"]

BOCHOPHOI = ["BoChoPhoi", "BoHauBiChoPhoi"]

TATCANHOMBO = {
    "tennhom": "bò",
    "danhsach": ["BoDucGiong", "Bo", "BoChuyenVoBeo", "Be", None],
}

TATCAGIOITINH = {
    "tennhom": "",
    "danhsach": ["Đực", "Cái", "Không xác định", None, ""],
}

TATCAPHANLOAI = {"tennhom":"","danhsach":["BoMoiPhoi",
        "BoMangThaiNho",
        "BoChoPhoi",
        "BoXuLySinhSan",
        "BoMeNuoiConNho",
        "BoChoDe",
        "BoMangThaiLon",
        "BoMeNuoiConLon",
        "BoVoBeoNho",
        "BoVoBeoTrung",
        "BoVoBeoLon",
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

TATCAPHANLOAI = {"tennhom":"","danhsach":["BoMoiPhoi",
        "BoMangThaiNho",
        "BoChoPhoi",
        "BoXuLySinhSan",
        "BoMeNuoiConNho",
        "BoChoDe",
        "BoMangThaiLon",
        "BoMeNuoiConLon",
        "BoHauBiChoPhoi",
        "BoHauBi",
]}


BE = {"tennhom":"bê","danhsach":[
        "BeCaiSua",
        "BeTheoMe",
        "BeSinh",
]}


BOGIONG  = {"tennhom":"bò giống","danhsach":["BoMoiPhoi",
        "BoMangThaiNho",
        "BoChoPhoi",
        "BoXuLySinhSan",
        "BoMeNuoiConNho",
        "BoChoDe",
        "BoMangThaiLon",
        "BoMeNuoiConLon",
        "BoHauBiChoPhoi",
        "BoHauBi",
        "BeCaiSua",
        "BeTheoMe",
        "BeSinh",
]}

BOVOBEO = {"tennhom":"bò vỗ béo","danhsach":[
        "BoVoBeoNho",
        "BoVoBeoTrung",
        "BoVoBeoLon",
        "BoNuoiThitBCT",
        "BoNuoiThitBCT8_12",
]}


DANGDIEUTRI = {
    "tennhom":"đang điều trị",
    "danhsach":["ChuaKham","DangDieuTri","DaKham"]
}