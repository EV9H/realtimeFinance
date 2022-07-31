
'''
-----------------------------------------------------------------------------
get yahoo finance data (replaced by polygon.io API to get minute level stats)
-----------------------------------------------------------------------------'''
# startDateStr = '2022-07-24'
# endDateStr = '2022-07-30'
# instrumentIds = ['IBM']
# ds = yf.download(tickers='TSLA',
#                  start=startDateStr, end=endDateStr, interval= "1m")
# stockData = ds['Adj Close'].to_frame()


'''
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
'''


'''---------------------------------------------------------------------------------------------'''
''' EXAMPLE: WRITING PANDAS DATAFRAME (YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ)
_now = datetime.utcnow()
_data_frame = pd.DataFrame(data=[["coyote_creek", 1.0], ["coyote_creek", 2.0]],
                           index=[_now, _now + timedelta(hours=1)],
                           columns=["location", "water_level"])

_write_client.write("my-bucket", "my-org", record=_data_frame, data_frame_measurement_name='h2o_feet',
                    data_frame_tag_columns=['location'])
'''
# IMPLEMENTATION
# stockData.index = stockData.index.strftime('%Y-%m-%dT%H:%M:%S.%f-05:00')
#print(stockData.index)
#write_api.write(bucket = bucket, org="evanhuanghz@gmail.com", record=stockData, data_frame_measurement_name='Adj Close')


'''---------------------------------------------------------------------------------------------'''

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




'''--------------------------------------------------------------------------------------------'''

'''
Query
'''

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
