# Database
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Stock API
# pip install polygon-api-client
from polygon import RESTClient
# pip install yfinance --upgrade --no-cache-dir
# import yfinance as yf

# Utility
from datetime import datetime, timedelta
import time
import pandas

# TEST
import random


'''
HELPER METHODS
'''


def convert_data_to_pd_dataframe(data):
    # Convert response(list) to pandas dataframe, acquire "timestamp" (epoch ms time) as index, "close" as value.
    df_raw = pandas.DataFrame(data=data)
    df_raw.index = df_raw['timestamp']
    df = df_raw['close'].to_frame()
    df.index = pandas.to_datetime(df.index, unit="ms")
    return df


'''
-----------------------------------------------------------------------------
Get Historical Data via Polygon API 
* Return
    dataframe containing timestamp and close price
* Parameters 
    ticker: <str>
    start_date: <str> "%YYYY-%mm-%dd" 
    precision: <str> "minute" / "hour" / "day" / ...
-----------------------------------------------------------------------------'''


def getHistoricalData(ticker, start_date, precision="minute"):
    polygon_api = "XKnXQHg1xz6to_UhGhpxvEbP14DL3_jO"
    polygon_client = RESTClient(polygon_api)
    data = []

    end_date = datetime.today().strftime("%Y-%m-%d")
    time_temp = datetime.strptime(start_date, "%Y-%m-%d")

    # Polygon seems only accepts up to 12 days in minute level data per request,
    # thus use loop to get each day's data
    for i in range(30):
        time_temp_str = time_temp.strftime("%Y-%m-%d")
        try:
            data.extend(polygon_client.get_aggs(ticker, 1, precision, time_temp_str, time_temp_str))
        except Exception:
            # Suppress no result error in non-trading days
            pass
        time_temp += timedelta(days=1)

    df = convert_data_to_pd_dataframe(data)
    return df


'''
-----------------------------------------------------------------------------
Update today's data once per minute
* Return
    dataframe containing timestamp and close price
* Parameters 
    ticker: <str>
    precision: <str> "minute" / "hour" / "day" / ...
-----------------------------------------------------------------------------'''


def updateData(ticker, precision="minute"):
    polygon_api = "XKnXQHg1xz6to_UhGhpxvEbP14DL3_jO"
    polygon_client = RESTClient(polygon_api)
    data = []
    date = datetime.today().strftime("%Y-%m-%d")
    try:
        data = polygon_client.get_aggs(ticker, 1, precision, date, date)
        df = convert_data_to_pd_dataframe(data)
        return df
    except Exception:
        pass
    return None


'''
-----------------------------------------------------------------------------
InfluxDB Set-up
-----------------------------------------------------------------------------'''

# token = os.environ.get("INFLUXDB_TOKEN")
token = "37t_iSK2DrWywrfKUpoVIgFGiAwVSeyI3dqlpk_WY9UVGU88pUKS33jO5rIoBun7WryXddgXgUWLhrXWxkXmNw=="
org = "evanhuanghz@gmail.com"
url = "https://us-east-1-1.aws.cloud2.influxdata.com"
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
bucket = "randomWalk"
write_api = client.write_api(write_options=SYNCHRONOUS)

'''
-----------------------------------------------------------------------------
SETTINGS
-----------------------------------------------------------------------------'''

STARTED = False
TEST_MODE = ~STARTED

START_DATE = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
TICKER = "AAPL"
INFLUXDB_MEASUREMENT_NAME = TICKER + " Close Price"

'''
-----------------------------------------------------------------------------
START 
-----------------------------------------------------------------------------'''

prompt = input("Start writing? (Y/N)")
if prompt == "Y" or prompt == "y":
    STARTED = True

if STARTED:
    # Historical Data
    historicalRecord = getHistoricalData(TICKER, START_DATE)
    write_api.write(bucket=bucket,
                    org=org,
                    record=historicalRecord,
                    data_frame_measurement_name=INFLUXDB_MEASUREMENT_NAME)

    # Update Real time Data (delayed by 15minutes)
    while STARTED:
        nowRecord = updateData(TICKER)
        if nowRecord is not None:
            print("Updated. Time: " + datetime.now().strftime("%Y%m%d% %H:%M:%S %Z"))
            write_api.write(bucket=bucket,
                            org=org,
                            record=nowRecord,
                            data_frame_measurement_name=INFLUXDB_MEASUREMENT_NAME)
        else:
            print("No response. Possibly because today is not a trading day.")
        time.sleep(60)

if TEST_MODE:
    historicalRecord = getHistoricalData(TICKER, START_DATE)
    print(historicalRecord)
