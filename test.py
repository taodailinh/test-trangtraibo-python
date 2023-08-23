import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


os.environ["PATH"] += "C:/Users/taoda/test/selenium/env"

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

driver.implicitly_wait(10)

driver.get("https://test-trangtraibo.aristqnu.com/quanlydan/danhsachdan")

# driver.execute_script(script)

cowCounter = driver.find_element(By.CLASS_NAME, "e-pagecountmsg")
print(cowCounter.text)

driver.quit()
