from openpyxl import Workbook
import time

wb = Workbook()
ws = wb.active

current = 0

ws.cell(current + 1, 1).value = "hello"

wb.save("testexcel.xlsx")
