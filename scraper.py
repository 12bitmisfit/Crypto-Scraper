# required pip installs are requests, pandas, and pyarrow
# you can change df.to_feather to something else if you don't want to use pyarrow
# feathers are the fastest and smallest though

import json
import requests
import pandas as pd
import time


def scraper():
    master_data = {"Time": 0,
                   "Price": 0,
                   "Ask_Price": [],
                   "Ask_Vol": [],
                   "Bid_Price": [],
                   "Bid_Vol": [],
                   "Open_24h": 0,
                   "High_24h": 0,
                   "Vol_24h": 0
                   }
    # You can change aws to www for faster responses but www was re-sending data to me a lot
    # aws does too, just much less often
    url = 'https://aws.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP'
    url2 = 'https://aws.okx.com/api/v5/market/books?instId=BTC-USDT&sz=400'
    data = json.loads(requests.get(url).text)
    books = json.loads(requests.get(url2).text)
    books = books['data'][0]
    data = data['data'][0]

    for i in range(len(books['asks'])):
        master_data['Ask_Price'].append(float(books['asks'][i][0]))
        master_data['Ask_Vol'].append(float(books['asks'][i][1]))
        master_data['Bid_Price'].append(float(books['bids'][i][0]))
        master_data['Bid_Vol'].append(float(books['bids'][i][1]))

    master_data['Time'] = int(data['ts'])
    master_data['Price'] = float(data['last'])
    master_data['Open_24h'] = float(data['open24h'])
    master_data['High_24h'] = float(data['high24h'])
    master_data['Vol_24h'] = float(data['vol24h'])

    return master_data


while True:
    starttime = time.time()
    d = scraper()
    df = pd.DataFrame.from_dict([d])
    df.to_feather("C:/feathers/{} btc-usdt.feather".format(d['Time']))
    print(time.time() - starttime)
