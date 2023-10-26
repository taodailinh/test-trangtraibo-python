from pymongo import MongoClient
from datetime import datetime, timedelta
import time


# Lấy giai đoạn bò bằng số tai
def tinhgiaidoanbo(client,db,datetocheck,sotai):
    collection = client[db]["BoNhapTrai"]
    bo = collection.find_one({"SoTai":sotai})
    ngaydecuoicung = None
    ngaykhamthaicuoicung = None
    ketquakhamgannhat = ""
    ngayphoicuoicung = None
    ngayxulysinhsancuoicung = None

    timelinenhangiong = []

    if len(bo["ThongTinSinhSans"]) != 0:
        lansinhcuoicung = max(bo["ThongTinSinhSans"], key=lambda x: x["NgaySinh"])
        ngaydecuoicung = lansinhcuoicung["NgaySinh"]
        print("Ngày đẻ gần nhất: "+str(ngaydecuoicung))

    if len(bo["ThongTinKhamThais"]) != 0:
        lankhamgannhat = max(bo["ThongTinKhamThais"], key=lambda x: x["NgayKham"])
        ngaykhamthaicuoicung = lankhamgannhat["NgayKham"]
        ketquakhamgannhat = lankhamgannhat["KetQuaKham"]
        print("Ngày khám thai gần nhất: "+str(ngaykhamthaicuoicung)+" với kết quả "+ketquakhamgannhat)

    if len(bo["ThongTinPhoiGiongs"]) != 0:
        lanphoigannhat = max(bo["ThongTinPhoiGiongs"], key=lambda x: x["NgayPhoi"])
        ngayphoicuoicung = lanphoigannhat["NgayPhoi"]
        print("Ngày phối gần nhất: "+str(ngayphoicuoicung))

    if len(bo["XuLySinhSans"]) != 0:
        # lanxulygannhat = max(bo["XuLySinhSans"], key=lambda x: max(x["LieuTrinhApDungs"], key=lambda y: y["NgayThucHien"]))
        ngayxlcuoimoidot = []
        for xuly in bo["XuLySinhSans"]:
            dotcuoi = max(xuly["LieuTrinhApDungs"], key=lambda x: x["NgayThucHien"])
            ngayxlcuoimoidot.append(dotcuoi)
        ngayxulysinhsancuoicung = max(ngayxlcuoimoidot, key=lambda x: x["NgayThucHien"])["NgayThucHien"]
        # print(ngaycuoixlssmoidot)
        # ngayphoicuoicung = lanphoigannhat["NgayThucHien"]
        print("Ngày xử lý gần nhất: "+str(ngayxulysinhsancuoicung))

    print("Hoàn thành số tai " +sotai)


    
# Tính giai đoạn bò của 1 con bò bằng số tai
    # Ngày thực hiện
    # Ngày tạo
    # Loại hoạt động
    # id
# Lần đẻ gần nhất
# Lần phối gần nhất
# Lần xlss gần nhất
# Lần khám thai gần nhất
# Hoạt động gần nhất:
    # Đẻ
        # Xem xét ngày đẻ
    # Phối giống
        # => Bò mới phối
    # Ghép đực
        # Bò mới phói
    # Xlss
        # Bò xử lý sinh sản
    # Khám thai
        # Có thai
            # Xem ngày phối gần nhất
        # Nghi có thai
            # Bò mới phối
        # Không có thai
            # Bò chờ phối
        # Chưa đủ ngày
            # Bò mới phối
    # Không có hoạt động nhân giống
        # Xét nhóm bò:
            # Bò thịt đực giống
            # Bò, bê

# Tính giai đoạn bò của toàn bộ bò