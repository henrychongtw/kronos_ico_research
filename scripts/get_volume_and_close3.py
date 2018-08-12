#!usr/bin/python

import time
import datetime
import os
import sys
import getopt
import csv

from coinapi_v1 import CoinAPIv1


def get_volume_and_close(target_symbol_name, targetdate, api, filepath):
    # time ----
    time_start = targetdate
    time_end = datetime.date(2018, 8, 11)
    # time_end = targetdate + datetime.timedelta(days = 1) + datetime.timedelta(microseconds = -1)  # since the one request contains 100 data samples, and there are 24 hourly date sample in a day, we use 100 / 24 ~ 4
    # time_end =

    # result ----
    start_str = time_start.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = time_end.strftime("%Y-%m-%dT%H:%M:%S")
    parameters = {"period_id" : "1DAY", "time_start" : start_str, "time_end" : end_str}
    ohlcv_historical = api.ohlcv_historical_data(target_symbol_name, parameters)  # ==== DATA REQUEST ONLY HAPPENED HERE ====
    print(target_symbol_name)

    # save file
    if len(ohlcv_historical) > 0:
        filepath_sav = os.path.join(filepath, time_start.strftime("%Y%m%d"))
        if not os.path.isdir(filepath_sav):
            os.makedirs(filepath_sav)
        filename = ''.join([time_start.strftime("%Y%m%d"), "_", target_symbol_name, ".csv"])
        dest = os.path.join(filepath_sav, filename)


        csv_file = open(dest, "w", newline = "")
        writer = csv.writer(csv_file, delimiter = ",")
        writer.writerow(["time_period_end", "volume_traded", "price_open", "price_close", "price_high", "price_low", "trades_count"])
        for ohlcv in ohlcv_historical:
            # if datetime.datetime.strptime(ohlcv["time_period_start"][0:10], "%Y-%m-%d") == time_start:
            writer.writerow([ohlcv["time_period_end"][0:26], ohlcv["volume_traded"], ohlcv["price_open"], ohlcv["price_close"], ohlcv["price_high"], ohlcv["price_low"], ohlcv["trades_count"]])

            # else:
                # while datetime.datetime.strptime(ohlcv["time_period_start"][0:10], "%Y-%m-%d") != time_start:
        csv_file.close()
        print("[FINISH]: ", target_symbol_name, " in ", time_start.strftime("%Y%m%d"), ". (D)", sep = "")

        #     time_start = datetime.datetime.strptime(((time_start + datetime.timedelta(days = 100))).strftime("%Y-%m-%d"), "%Y-%m-%d")
        #     start_str = time_start.strftime("%Y-%m-%dT%H:%M:%S")
        #     parameters = {"period_id" : "1DAY", "time_start" : start_str, "time_end" : end_str}
        #     ohlcv_historical = api.ohlcv_historical_data(target_symbol_name, parameters)
        # elif end_str == start_str:
        #     csv_file.close()
        #     print("[FINISH]: ", target_symbol_name, " in ", time_start.strftime("%Y%m%d"), ". (U)", sep = "")
        #     break




def main(argv):
    # Parameter setting --------
    day_start_str = "20170607"
    day_end_str = "20180811"
    apikey = "6BEE3909-BA2B-49FA-8751-2A4D14F9FB6F"
    # 885A67CA-EA79-409F-B586-C72CAFAB2B7A
    #6BEE3909-BA2B-49FA-8751-2A4D14F9FB6F
    # apikey = "3DCACFD6-BD97-4FBF-9410-A4F0C6929154"  # This is my personal free one (100 rate limit). Please use this first for testing
    filepath = "/mnt/c/henryshawn/marketdata/ICO_data/cofoundit_data"
    symbol_id = ""

    opts, args = getopt.getopt(argv, "s:e:k:p:S:", ["start_day=", "end_day=", "api_key=", "path=", "symbol_id="])
    for opt, arg in opts:
        if opt in ("-s", "--start_day"):
            day_start_str = arg
        elif opt in ("-e", "--end_day"):
            day_end_str = arg
        elif opt in ("-k", "--api_key"):
            apikey = arg
        elif opt in ("-p", "--path"):
            filepath = arg
        elif opt in ("-S", "--symbol_id"):
            symbol_id = arg

    # Environment setting --------
    day_start = datetime.datetime.strptime(day_start_str, "%Y%m%d")
    day_end = datetime.datetime.strptime(day_end_str, "%Y%m%d")
    api = CoinAPIv1(apikey)
    time.sleep(0.01)  # use time.sleep to prevent from (104, 'Connection reset by peer')
    symbols_raw = api.metadata_list_symbols()
    symbols_list = []
    for symbol in symbols_raw:
        symbols_list.append(symbol["symbol_id"])

    # Calculating VWAP
    targetdate = day_start

    # Target asset information --------
    # target_exchange_list = ["BITHUMB", "OKEX","HUOBIPRO", "BINANCE","COINONE","GATEIO"]
    # target_instrument_list = ["SPOT"]
    # target_asset_list = ["KNC_KRW", "KNC_USDT", "KNC_BTC", "KNC_ETH" ]

    target_exchange_list = [ "HITBTC","BITTREX", "LIQUI"]
    target_instrument_list = ["SPOT"]
    target_asset_list = ["CFI_BTC", "CFI_ETH",  "CFI_USDT"]

    # target_exchange_list = [ "BINANCE"]
    # target_instrument_list = ["SPOT"]
    # target_asset_list = ["KNC_BTC" ]
    # target_asset_list = ["BTC_USD", "ETH_USD", "EOS_USD", "ETC_USD", "BCH_USD", "XRP_USD", "ETH_BTC"]

    while True:
        if symbol_id == "":
            for exchange in target_exchange_list:
                for instrument in target_instrument_list:
                    for asset in target_asset_list:
                        target_symbol_name = '_'.join([exchange, instrument, asset])
                        if target_symbol_name in symbols_list:
                            get_volume_and_close(target_symbol_name, targetdate, api, filepath)
                        else:
                            print("\033[91m", "====Warning====: [", targetdate.strftime("%Y%m%d"), "] There is no Symbol ID: ", target_symbol_name, ".", "\033[0m", sep = "")

        else:
            target_symbol_name = symbol_id
            if target_symbol_name in symbols_list:
                get_volume_and_close(target_symbol_name, day_start, day_end, api, filepath)
            else:
                print("\033[91m", "====Warning====: [", targetdate.strftime("%Y%m%d"), "] There is no Symbol ID: ", target_symbol_name, ".", "\033[0m", sep = "")

        targetdate = datetime.datetime.strptime(((targetdate + datetime.timedelta(days = 100))).strftime("%Y-%m-%d"), "%Y-%m-%d")
        if targetdate > day_end:
            break



if __name__ == "__main__":
    main(sys.argv[1:])
