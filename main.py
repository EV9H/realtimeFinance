import influxdb_client
import os
import time

import pandas
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
# pip install yfinance --upgrade --no-cache-dir

import yfinance as yf

# TEST FUNCTIONS
import random
from datetime import datetime, timedelta

# Initialize Client
# token = os.environ.get("INFLUXDB_TOKEN")
token = "37t_iSK2DrWywrfKUpoVIgFGiAwVSeyI3dqlpk_WY9UVGU88pUKS33jO5rIoBun7WryXddgXgUWLhrXWxkXmNw=="
org = "evanhuanghz@gmail.com"
url = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# get yahoo finance data
startDateStr = '2022-07-01'
endDateStr = '2022-07-30'
instrumentIds = ['IBM']
ds = yf.download(tickers='IBM',
                 start=startDateStr, end=endDateStr)
stockData = ds['Adj Close']

# Write Data
bucket = "financeData"

write_api = client.write_api(write_options=SYNCHRONOUS)

for timestamp in stockData.index:
    val = stockData[timestamp]
    timestamp_datetime_UTC = pandas.Timestamp.to_pydatetime(timestamp).isoformat("T") + "Z"
    print(timestamp_datetime_UTC, val)
    point = (
        Point("Stocks")
            .tag("Ticker", "IBM")
            .field("AdjClosed", val)
            .time(timestamp_datetime_UTC)
    )
    write_api.write(bucket=bucket, org="evanhuanghz@gmail.com", record=point)

    # time.sleep(1)  # separate points by 1 second

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

""" convert date time to utc """
# val = 9999
# datetime = datetime.utcnow() - timedelta(days = 2)
# time_minus_1d = datetime.isoformat("T") + "Z"
#
# point = (
#     Point("measurement1")
#     .tag("name", "random1")
#     .field("value", val)
#     .time(time_minus_1d)
# )
# write_api.write(bucket=bucket, org="evanhuanghz@gmail.com", record=point)

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
