#!usr/bin/python

import datetime
import os
import sys
import getopt
import csv
import pytz

from coinapi_v1 import CoinAPIv1

# Vesting periods
# organic hype
# liquidity initially
# holding percentage ( minority vs majority)
# price drop bring mm view into it
# manufactured hype
# Discounts
# Good exchange bad exchange

def main(argv):
    # Parameter setting --------
    day_start_str = (datetime.datetime.now().replace(tzinfo = pytz.timezone("Asia/Taipei")).astimezone(pytz.utc) - datetime.timedelta(days = 1)).strftime("%Y%m%d")
    # apikey = "6BEE3909-BA2B-49FA-8751-2A4D14F9FB6F"
    apikey = "885A67CA-EA79-409F-B586-C72CAFAB2B7A"
    filepath = "/mnt/c/henryshawn/marketdata/reference/"

    api = CoinAPIv1(apikey)

    opts, args = getopt.getopt(argv, "s:k:p:", ["start_day=", "api_key=", "path="])
    for opt, arg in opts:
        if opt in ("-s", "--start_day"):
            day_start_str = arg
        elif opt in ("-k", "--api_key"):
            apikey = arg
        elif opt in ("-p", "--path"):
            filepath = arg


    # Read from watch list (watch-list contains the asset pair name of currency pairs that we would like to monitor) (using daily_assets.YYYYMMDD file)
    watch_list = []
    if os.path.isfile("".join(["/mnt/c/henryshawn/marketdata/scripts/scripts_data/daily_assets.", day_start_str])):
        with open("".join(["/mnt/c/henryshawn/marketdata/scripts/scripts_data/daily_assets.", day_start_str]), newline = '') as csvfile:
            rows = csv.reader(csvfile)
            for row in rows:
                watch_list.append((row[0], row[1], row[2], row[3]))
    else:
        with open("".join(["/mnt/c/henryshawn/marketdata/scripts/scripts_data/daily_assets.", "20180809"]), newline = '') as csvfile:
            rows = csv.reader(csvfile)
            for row in rows:
                watch_list.append((row[0], row[1], row[2], row[3]))


    # Asset name-to-key mapping (using the assets.keys file)
    asset_name_to_key = {}
    with open('/mnt/c/henryshawn/marketdata/keys/assets.keys', newline = '') as csvfile:
        rows = csv.DictReader(csvfile, delimiter = ',')
        for row in rows:
            asset_name_to_key[row["asset_name"]] = row["asset_key"]


    # Environment setting --------
    day_start = datetime.datetime.strptime(day_start_str, "%Y%m%d")
    targetdate = day_start

    time_start = targetdate + datetime.timedelta(hours = 0)
    time_end = targetdate + datetime.timedelta(hours = 24)


    # result
    result = {}
    symbols = api.metadata_list_symbols()
    parameters = {"period_id" : "1DAY", "time_start" : time_start.strftime("%Y-%m-%dT%H:%M:%S"), "time_end" : time_end.strftime("%Y-%m-%dT%H:%M:%S")}

    number_of_request = 0  # DEBUG_MESSAGE
    for symbol in symbols:
        assetA_name = symbol["asset_id_base"]
        assetB_name = symbol["asset_id_quote"]

        if (not assetA_name in asset_name_to_key.keys()) or (not assetB_name in asset_name_to_key.keys()):
            break

        assetA_id = asset_name_to_key[assetA_name]
        assetB_id = asset_name_to_key[assetB_name]
        this_key = (assetA_id, assetB_id, assetA_name, assetB_name)

        if (this_key in watch_list) and (symbol["symbol_type"] == "SPOT"):
            ohlcv_historical = api.ohlcv_historical_data(symbol["symbol_id"], parameters)  # ==== DATA REQUEST NOLY HAPPENED HERE ====
            number_of_request += 1  # DEBUG_MESSAGE
            if not ohlcv_historical == []:
                if this_key in result.keys():
                   result[this_key][0] += ohlcv_historical[0]["volume_traded"]
                   # result[this_key][1] += ohlcv_historical[0]["volume_traded"] * ohlcv_historical[0]["price_close"]
                   result[this_key][1] += ohlcv_historical[0]["price_close"]
                else:
                   result[this_key] = [ohlcv_historical[0]["volume_traded"], ohlcv_historical[0]["volume_traded"] * ohlcv_historical[0]["price_close"]]

    print("For ", time_start.strftime("%Y-%m-%d"), ", we request the OHLCV data ", number_of_request, " times.", sep = "")  # DEBUG_MESSAGE


    filepath_save = os.path.join(filepath, targetdate.strftime("%Y%m%d"))
    if not os.path.isdir(filepath_save):
        os.makedirs(filepath_save)
    filename = "closing_prices.csv"
    dest = os.path.join(filepath_save, filename)
    with open(dest, "w", newline = "") as csvfile:
        writer = csv.writer(csvfile, delimiter = ",")
        writer.writerow(["assetA_id", "assetB_id", "assetA_name", "assetB_name", "closing_price","volume"])
        for a_result in result:
            assetA_id = a_result[0]
            assetB_id = a_result[1]
            assetA_name = a_result[2]
            assetB_name = a_result[3]
            closing_price = result[a_result][1]
            volume = result[a_result][0]
            writer.writerow([assetA_id, assetB_id, assetA_name, assetB_name, closing_price])



if __name__ == "__main__":
    main(sys.argv[1:])
