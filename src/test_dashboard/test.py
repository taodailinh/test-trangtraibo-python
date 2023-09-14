
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

os.environ["PATH"] += "C:/Users/taoda/test/selenium/env"





# Tìm 1 con bò bằng sô tai và in số chip
# for i in boNhapTrai.find({"SoTai": "F040923"}):
#     print(i["SoChip"])


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



wb = Workbook()


time.sleep(10)


time.sleep(10)

fileName = "test_dashboard" + datetime.now().strftime("%Y%B%d%H%M%S.xlsx")
wb.save(fileName)
# driver.quit()
