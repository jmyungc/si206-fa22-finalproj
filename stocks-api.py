import unittest
import sqlite3
import json
import os
import requests

API_KEY = "ET388N6DI4NR30RE"

def create_request(stock_symbol):
    symbol = stock_symbol

    request_url = "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol=" + stock_symbol + "&apikey=" + API_KEY

    return request_url

def call_api(request_url):
    req = requests.get(request_url)

    data_dict = {}
    data_dict = json.loads(req.text)

    print(data_dict['Weekly Adjusted Time Series']['2022-12-09']['2. high'])
    


def main():
    request = create_request("JNJ")
    print(request)
    call_api(request)



if __name__ == "__main__":
    main()


