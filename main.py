import influxdb_client
import os
import time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
# pip install yfinance --upgrade --no-cache-dir

import yfinance as yf

# TEST FUNCTIONS
import random
from datetime import datetime

# Initialize Client
# token = os.environ.get("INFLUXDB_TOKEN")
token = "37t_iSK2DrWywrfKUpoVIgFGiAwVSeyI3dqlpk_WY9UVGU88pUKS33jO5rIoBun7WryXddgXgUWLhrXWxkXmNw=="
org = "evanhuanghz@gmail.com"
url = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# get yahoo finance data
startDateStr = '2022-07-25'
endDateStr = '2022-07-30'
instrumentIds = ['IBM']
ds = yf.download(tickers='IBM',
                 start=startDateStr,  end=endDateStr)
data = ds['Adj Close']

for index in data.index:
    print(index)
    print(data[index])

# Write Data
bucket = "randomWalk"

write_api = client.write_api(write_options=SYNCHRONOUS)



# for value in data:
#     point = (
#         Point("measurement1")
#         .tag("Ticker", "IBM")
#         .field("AdjClosed", value)
#     )
#     write_api.write(bucket=bucket, org="evanhuanghz@gmail.com", record=point)
#     print(value)
#     time.sleep(0.1)  # separate points by 1 second

'''
start = 0
while True:

    newValue = start + random.randint(-10, 10)
    start = newValue
    point = (
            Point("measurement1")
            .tag("name", "random1")
            .field("value", newValue)
        )
    write_api.write(bucket=bucket, org="evanhuanghz@gmail.com", record=point)

    time.sleep(5)  # separate points by 1 second
'''

val = random.randint(0,100)
data = (
    Point("measurement1")
    .tag("name", "random1")
    .field("value", val)
    .time(datetime.datetime.now())
)



# # Flux Query
#
# query_api = client.query_api()
#
# query = """from(bucket: "financeData")
#  |> range(start: -10m)
#  |> filter(fn: (r) => r._measurement == "measurement1")
#  """
# tables = query_api.query(query, org="evanhuanghz@gmail.com")
#
# for table in tables:
#     print(table)
#     for record in table.records:
#         print(record)
