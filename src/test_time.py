from datetime import datetime, timedelta

currentDate = datetime(2023,10,16,11,16,30)

fiveDayAgo = datetime(2023,10,11,11,18,30)

daytheshold = currentDate-timedelta(days = 5)

print(currentDate-fiveDayAgo)
print(daytheshold)
# now = datetime.now

