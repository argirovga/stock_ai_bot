
# ML


# DL


# basic
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

import requests

import datetime

import json, zipfile
from io import BytesIO 

import os, sys

# extra



# global_parameters
historical_URL = ""
new_URL = ""

class StocksLoader():

    tickers: list
    token: str


    def __init__(self, tickers_to_load: list, token: str):
        self.tickers = tickers_to_load
        self.token = token

    def check_time(self, current_time_utc):
        if current_time_utc.weekday() >= 5:
            days_to_friday = (current_time_utc.weekday() - 4) % 7
            last_friday = current_time_utc - datetime.timedelta(days=days_to_friday)
            adjusted_time = last_friday.replace(hour=20, minute=50, second=0, microsecond=0)
            return False, adjusted_time
        
        # IMOEX standard working hours: 9:50 - 23:50
        start_time = current_time_utc.replace(hour=6, minute=50, second=0, microsecond=0)
        end_time = current_time_utc.replace(hour=20, minute=50, second=0, microsecond=0)


        night_time = current_time_utc.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        if start_time <= current_time_utc <= end_time:
            return True, current_time_utc
        else:
            # If it is night or weekend set to latest working hours (Friday, 20:50)
            if current_time_utc.weekday() == 1:
                last_friday = current_time_utc - datetime.timedelta(days=3)
                adjusted_time = last_friday.replace(hour=20, minute=50, second=0, microsecond=0)
            else:
                if night_time >= current_time_utc > end_time:
                    adjusted_time = current_time_utc.replace(hour=20, minute=50, second=0, microsecond=0)
                elif current_time_utc < start_time:
                    yesterday = current_time_utc - datetime.timedelta(days=1)
                    adjusted_time = yesterday.replace(hour=20, minute=50, second=0, microsecond=0)
            return False, adjusted_time


    def get_figi(self, ticker):
        instruments_json_path = os.path.join(os.getcwd(), 'data', 'shares_imoex.json')
        with open(instruments_json_path, encoding="utf8") as f:
            instruments_temp = json.load(f)["instruments"]
            for instr_temp in instruments_temp:
                if instr_temp['ticker'] == ticker:
                    return instr_temp['figi']

        return self.get_figi_extra(ticker)
    
    def get_figi_extra(self, ticker):
        base_url = "https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/Shares"

        payload = {
            "instrumentStatus": "INSTRUMENT_STATUS_BASE"
        }
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        response = requests.post(base_url, json=payload, headers=headers)

        if response.status_code == 200:
            data_instruments = response.json().get("instruments")
            for data_instrument in data_instruments:
                if data_instrument.get("ticker") == ticker:
                    return data_instrument.get("figi")
        
        return None

    def get_historical_data(self, start_year: int, end_year: int = 2025) -> list:
        headers = {"Authorization": f"Bearer {self.token}"}
        base_url = "https://invest-public-api.tinkoff.ru/history-data"

        stocks_left = []
        dfs = []

        for ticker in self.tickers:
            ticker_data_df = pd.DataFrame()
            for year in range(start_year, end_year + 1):
                figi = self.get_figi(ticker)
                if not figi:
                    raise ValueError("Failed to get figi from T-bank")
                params = {"figi": figi, "year": year}
                response = requests.get(base_url, params=params, headers=headers)

                if response.status_code == 200:
                    print(f"Started parsing instrument: {ticker} for {year}")
                    with zipfile.ZipFile(BytesIO(response.content)) as archive:
                        for register_file_name in archive.namelist():
                            with archive.open(register_file_name) as register_file:
                                df = pd.read_csv(register_file, sep=';', header=None)
                                df.columns = ['uid', 'date_time_start', 'Open', 'Close', 'High', 'Low', 'Volume', 'Spare']
                                if ticker_data_df.empty:
                                    ticker_data_df = df
                                else:
                                    ticker_data_df = pd.concat([ticker_data_df, df], axis=0)

                    print(f"Finished parsing instrument: {ticker} for {year}")

                elif response.status_code == 429:
                    stocks_left.append((figi, ticker))
                    time.sleep(30)
                    print(f"Rate limit exceeded, stopped for a while (30 seconds)")
            if not ticker_data_df.empty:
                ticker_data_df['ticker'] = ticker
                ticker_data_df['figi'] = figi
                dfs.append(ticker_data_df)
                print(f"Added concatenated data for {ticker}")

        if stocks_left:
            print(f"Some stocks hit rate limit, retry later for: {stocks_left}")
        
        return dfs

    def get_latest_data(self, delta_days: int = 4) -> list:
        base_url = "https://invest-public-api.tinkoff.ru/rest/tinkoff.public.invest.api.contract.v1.MarketDataService/GetCandles"
        
        time_valid, current_time_utc = self.check_time(datetime.datetime.now(datetime.timezone.utc))

        # for debug purposes set to a workday
        # current_time_utc = current_time_utc - datetime.timedelta(days=3, hours=6)
        
        start_time_utc = (current_time_utc - datetime.timedelta(hours=24 * delta_days)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        end_time_str = current_time_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        dfs = []

        for ticker in self.tickers:
            figi = self.getfigi(ticker)
            if not figi:
                raise ValueError("Failed to get figi from T-bank")

            payload = {
                "figi": figi,
                "from": start_time_utc,
                "to": end_time_str,
                "interval": "CANDLE_INTERVAL_5_MIN"
            }
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(base_url, json=payload, headers=headers)

            if response.status_code == 200:
                data = response.json().get('candles')
                candles_data = []

                for candle in data:
                    if candle["isComplete"]:
                        candle_data = {
                            "Open": float(candle["open"].get("units")) + float('0.' + str(candle["open"].get("nano", 0))),
                            "Close": float(candle["close"].get("units")) + float('0.' + str(candle["close"].get("nano", 0))),
                            "High": float(candle["high"].get("units")) + float('0.' + str(candle["high"].get("nano", 0))),
                            "Low": float(candle["low"].get("units")) + float('0.' + str(candle["low"].get("nano", 0))),
                            "Volume": int(candle["volume"]),
                            "DateTime": candle["time"],
                        }
                        candles_data.append(candle_data)
                temp_df = pd.DataFrame(candles_data)
                temp_df['ticker'] = ticker
                temp_df['figi'] = figi
                dfs.append(temp_df)
            else:
                raise ValueError("Failed to load data from T-bank")

        return dfs
    
