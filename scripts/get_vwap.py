#!/usr/bin/python

import time  # for time.sleep()
import datetime
import sys
import getopt
import csv
import os
import logging
import gzip
import shutil

from coinapi_v1 import CoinAPIv1


# function cal_vwap
def cal_vwap(target_symbol_name, targetdate, api, filepath):
    # header for the output data
    result = [["time_exchange_hour","volume_prior_to_this_hour","vwap_prior_to_this_hour"]]

    # limit: 100000 is the maximum
    limit = 100000
    
    # trades time horizon
    time_start = targetdate
    time_end = targetdate + datetime.timedelta(hours = 24)
    
    # request trades data from the api http
    time.sleep(0.01)  # use time.sleep to prevent from (104, 'Connection reset by peer')
    historical_trades_raw = api.trades_historical_data(target_symbol_name, {'time_start': time_start.strftime("%Y-%m-%dT%H:%M:%S"), 'time_end': time_end.strftime("%Y-%m-%dT%H:%M:%S"), 'limit': limit})
    while len(historical_trades_raw) == limit:
        time_start = datetime.datetime.strptime(historical_trades_raw[-1]["time_exchange"][0:26], "%Y-%m-%dT%H:%M:%S.%f")
        time.sleep(0.01)  # use time.sleep to prevent from (104, 'Connection reset by peer')
        historical_trades_raw += api.trades_historical_data(target_symbol_name, {'time_start': time_start.strftime("%Y-%m-%dT%H:%M:%S"), 'time_end': time_end.strftime("%Y-%m-%dT%H:%M:%S"), 'limit': limit})
        
    # delete duplicated trades data
    seen = set()
    historical_trades = []
    for d in historical_trades_raw:
        t = tuple(d.items())
        if t not in seen:
            seen.add(t)
            historical_trades.append(d)

    # save trade data file
    if len(historical_trades) != 0:
        filepath_trade = os.path.join("/marketdata/trades", targetdate.strftime("%Y%m%d"))
        if not os.path.isdir(filepath_trade):
            os.makedirs(filepath_trade)
        
        filename_trd = ''.join([targetdate.strftime("%Y%m%d"), "-", target_symbol_name, ".csv"])
        dest = os.path.join(filepath_trade, filename_trd)
        if not os.path.isfile(dest):
            with open(dest, 'w', newline = '') as csvfile:
                writer = csv.writer(csvfile, delimiter = ";")
                writer.writerow(["time_exchange", "time_coinpai", "guid", "price", "base_amount", "taker_side"])
                for trade in historical_trades:
                    writer.writerow([trade["time_exchange"][0:26], trade["time_coinapi"][0:26], trade["uuid"], trade["price"], trade["size"], trade["taker_side"]])
            
        # compress the trade data file         
        filename_cps = ''.join([filename_trd, ".gz"])
        dest_cps = os.path.join(filepath_trade, filename_cps)
        if not os.path.isfile(dest_cps):
            with open(dest, 'rb') as fi, gzip.open(dest_cps, 'wb') as fo:
                shutil.copyfileobj(fi, fo)
            
            
    # quotes time horizon
    time_start = targetdate
    time_end = targetdate + datetime.timedelta(hours = 24)
    
    # request quotes data from the api http
    time.sleep(0.01)  # use time.sleep to prevent from (104, 'Connection reset by peer')
    historical_quotes_raw = api.quotes_historical_data(target_symbol_name, {'time_start': time_start.strftime("%Y-%m-%dT%H:%M:%S"), 'time_end': time_end.strftime("%Y-%m-%dT%H:%M:%S"), 'limit': limit})
    while len(historical_quotes_raw) == limit:
        time_start = datetime.datetime.strptime(historical_quotes_raw[-1]["time_exchange"][0:26], "%Y-%m-%dT%H:%M:%S.%f")
        time.sleep(0.01)  # use time.sleep to prevent from (104, 'Connection reset by peer')
        historical_quotes_raw += api.trades_historical_data(target_symbol_name, {'time_start': time_start.strftime("%Y-%m-%dT%H:%M:%S"), 'time_end': time_end.strftime("%Y-%m-%dT%H:%M:%S"), 'limit': limit})
        
    # delete duplicated quotes data
    seen = set()
    historical_quotes = []
    for d in historical_quotes_raw:
        t = tuple(d.items())
        if t not in seen:
            seen.add(t)
            historical_quotes.append(d)

    
    
    # If historical_trades is not empty
    if len(historical_trades) != 0:
        time_end = targetdate + datetime.timedelta(hours = 1)
        time_end_str = time_end.strftime("%Y-%m-%dT%H:%M:%S.%f")

        volume = 0
        volume_price = 0    
        
        for trade in historical_trades:            
            if trade["time_exchange"][0:26] > time_end_str:
                if volume > 0:
                    result.append([time_end_str, volume, volume_price / volume])
#                    print(time_end_str, volume, volume_price / volume)  # for debug
                else:
                    for quote in historical_quotes:
                        bid_price = 0
                        ask_price = 0
                        if trade["time_exchange"][0:26] > time_end_str:
                            break
                        else:
                            bid_price = quote['bid_price']
                            ask_price = quote['ask_price']
                    result.append([time_end_str, volume, 0.5 * (bid_price + ask_price)])
#                    print(time_end_str, volume, 0.5 * (bid_price + ask_price))  # for debug
                    
                volume = 0
                volume_price = 0

                while trade["time_exchange"][0:26] > time_end_str:
                    time_end += datetime.timedelta(hours = 1)
                    time_end_str = time_end.strftime("%Y-%m-%dT%H:%M:%S.%f")
                    
            volume += trade['size']
            volume_price += trade['size'] * trade['price']
            
        if volume > 0:
            result.append([time_end_str, volume, volume_price / volume])
#            print(time_end_str, volume, volume_price / volume)  # for debug
        
        # save file
        filepath = os.path.join(filepath, targetdate.strftime("%Y%m%d"))
        if not os.path.isdir(filepath):
            os.makedirs(filepath)
            logging.warning(''.join(["Create a directory: ", filepath]))
        filename = ''.join([targetdate.strftime("%Y%m%d"), "_", target_symbol_name, ".csv"])
        with open(os.path.join(filepath, filename), 'w', newline = '') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(result)
        print("[FINISH]: ", target_symbol_name, " in ", targetdate.strftime("%Y%m%d"), ".", sep = "")
            
    else:
        # save file
        filepath = os.path.join(filepath, targetdate.strftime("%Y%m%d"))
        if not os.path.isdir(filepath):
            os.makedirs(filepath)
            logging.warning(''.join(["Create a directory: ", filepath]))
        filename = ''.join([targetdate.strftime("%Y%m%d"), "_", target_symbol_name, "_(empty).csv"])
        with open(os.path.join(filepath, filename), 'w', newline = '') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(result)     
        
        print("\033[93m", "[CAUTION]: No historical trades for ", target_symbol_name, " in ", targetdate.strftime("%Y%m%d"), ".", "\033[0m", sep = "")



# function main----
def main(argv):   
    # Parameter setting --------
    day_start_str = "20160101"
    day_end_str = "20180701"
    apikey = "6BEE3909-BA2B-49FA-8751-2A4D14F9FB6F"
    filepath = "/marketdata/vwap"
    symbol_id = ""
    
    logging.basicConfig(stream = sys.stdout, level = logging.DEBUG)
    
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
    target_exchange_list = ["BINANCE", "BITTREX", "BITFINEX", "COINBASE", "KUCOIN", "HUOBI", "OKEX"]
    target_instrument_list = ["SPOT"]
    target_asset_list = ["BTC_USD", "ETH_USD", "EOS_USD", "ETC_USD"]
    # target_asset_list = ["BTC_USD", "ETH_USD", "EOS_USD", "ETC_USD", "BCH_USD", "XRP_USD", "ETH_BTC"]

    while True:
        if symbol_id == "":        
            for exchange in target_exchange_list:
                for instrument in target_instrument_list:
                    for asset in target_asset_list:
                        target_symbol_name = '_'.join([exchange, instrument, asset])
                        if target_symbol_name in symbols_list:
                            cal_vwap(target_symbol_name, targetdate, api, filepath)
                        else:
                            print("\033[91m", "========Warning========: There is no Symbol ID: ", target_symbol_name, ".", "\033[0m", sep = "")
                        
        else:
            target_symbol_name = symbol_id
            if target_symbol_name in symbols_list:
                cal_vwap(target_symbol_name, targetdate, api, filepath)
            else:
                print("\033[91m", "========Warning========: There is no Symbol ID: ", target_symbol_name, ".", "\033[0m", sep = "")

        targetdate += datetime.timedelta(days = 1)
        if targetdate > day_end:
            break



# process
if __name__ == "__main__":
    main(sys.argv[1:])
