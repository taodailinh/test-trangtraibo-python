from selenium.webdriver.common.by import By


def countCow(driver, className):
    cowCounter = driver.find_element(By.CLASS_NAME, className)
    danhSachDan = cowCounter.text[
        cowCounter.text.find("(") + 1 : cowCounter.text.find(" ")
    ]
    return int(danhSachDan)
