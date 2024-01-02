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
        timelinenhangiong.append({
            "NgayThucHien":lansinhcuoicung["NgaySinh"],
            "NgayTao":lansinhcuoicung["NgaySinh"],
            "HangMuc":"Đẻ",
            "KetQua":{
                "SoTaiBe":lansinhcuoicung["SoTaiBe"]
            }
        })

    if len(bo["ThongTinKhamThais"]) != 0:
        lankhamgannhat = max(bo["ThongTinKhamThais"], key=lambda x: x["NgayKham"])
        ngaykhamthaicuoicung = lankhamgannhat["NgayKham"]
        ketquakhamgannhat = lankhamgannhat["KetQuaKham"]
        print("Ngày khám thai gần nhất: "+str(ngaykhamthaicuoicung)+" với kết quả "+ketquakhamgannhat)
        timelinenhangiong.append({
            "NgayThucHien":ngaykhamthaicuoicung,
            "NgayTao":ngaykhamthaicuoicung,
            "HangMuc":"Khám thai",
            "KetQua":{
                "KetQuaKham":lankhamgannhat["KetQuaKham"]
            }
        })


    if len(bo["ThongTinPhoiGiongs"]) != 0:
        lanphoigannhat = max(bo["ThongTinPhoiGiongs"], key=lambda x: x["NgayPhoi"])
        ngayphoicuoicung = lanphoigannhat["NgayPhoi"]
        print("Ngày phối gần nhất: "+str(ngayphoicuoicung))
        timelinenhangiong.append({
            "NgayThucHien":ngayphoicuoicung,
            "NgayTao":lanphoigannhat["CreatedAt"],
            "HangMuc":"Phối giống",
            "KetQua":{
                "VeDuc":lanphoigannhat["GhepDuc"],
                "VeDucKhongQuaPhoi":lanphoigannhat["GhepDucKhongQuaPhoi"],
                "NgayVeDuc":lanphoigannhat["NgayGhepDuc"],                
                "LanPhoi":lanphoigannhat["LanPhoi"]
            }
        })

    if len(bo["XuLySinhSans"]) != 0:
        # lanxulygannhat = max(bo["XuLySinhSans"], key=lambda x: max(x["LieuTrinhApDungs"], key=lambda y: y["NgayThucHien"]))
        buocxlcuoimoidot = []

        for xuly in bo["XuLySinhSans"]:
            buocxlcuoi = max(xuly["LieuTrinhApDungs"], key=lambda x: x["NgayThucHien"])
            buocxlcuoimoidot.append({"ThongTinBuocXL":buocxlcuoi,"idDotXuLy":xuly["_id"]})
        xulycuoi = max(buocxlcuoimoidot, key=lambda x: x["ThongTinBuocXL"]["NgayThucHien"])
        ngayxulycuoi = xulycuoi["ThongTinBuocXL"]["NgayThucHien"]
        idDotXuLyCuoi = xulycuoi["idDotXuLy"]
        # print(ngaycuoixlssmoidot)
        # ngayphoicuoicung = lanphoigannhat["NgayThucHien"]
        print("Ngày xử lý gần nhất: "+str(ngayxulycuoi))
        timelinenhangiong.append({
            "NgayThucHien":ngayxulycuoi,
            "HangMuc":"Xử lý sinh sản",
            "KetQua":{
                "idDotXuLy":idDotXuLyCuoi,
            }
        })
    if len(timelinenhangiong) != 0:
        sortedtimeline = sorted(timelinenhangiong,key=lambda x:x["NgayThucHien"],reverse=True)
        khamthai = (x for x in sortedtimeline if x["HangMuc"]=="Khám thai")
        khamthaigannhat = next(khamthai) # lần khám thai gần nhất

        phoigiong = (x for x in sortedtimeline if x["HangMuc"]=="Phối giống")
        phoigionggannhat = next(phoigiong) # lần phối giống gần nhất

        xlss = (x for x in sortedtimeline if x["HangMuc"]=="Xử lý sinh sản")
        xlssgannhat = next(xlss) # lần xử lý sinh sản gần nhât

        de = (x for x in sortedtimeline if x["HangMuc"]=="Đẻ")
        degannhat = next(de) # lần đẻ gần nhât



        print("Công việc cuối: "+sortedtimeline[0]["HangMuc"])
        if sortedtimeline[0]["HangMuc"] == "Khám thai":
            print(sortedtimeline[0]["KetQua"]["KetQuaKham"])
            print("Ngày có thai: "+str(phoigionggannhat["NgayThucHien"]))
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