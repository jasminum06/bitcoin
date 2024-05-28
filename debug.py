# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 16:17:01 2024

@author: lynie
"""
import pandas as pd
import json


def json_to_df(quote_data:dict):
    # result: same as csv: columns = time,market,level,coin_metrics_id,database_time,
    # ask_price,ask_size,bid_price,bid_size
    # convert to pd.DataFrame:
    del quote_data['market'],quote_data['ask'],quote_data['bid']
    
    for key, value in quote_data.items():
        if key == '2023-10-02':
            dfs = []
            for i in range(len(value)):
                df = pd.DataFrame()
                df[['ask_price', 'ask_size']] = pd.DataFrame(value[i]['asks']).astype(float).values
                df[['bid_price', 'bid_size']] = pd.DataFrame(value[i]['bids']).astype(float).values
                df['level'] = [f'level_{j+1}' for j in range(100)]
                df.loc[:,'time'] = value[i]['time']
                df.loc[:,'market'] = value[i]['market']
                df.loc[:,'coin_metrics_id'] = value[i]['coin_metrics_id']
                df.loc[:,'database_time'] = value[i]['database_time']
                
                dfs.append(df)
          
            # quote_data_daily = pd.concat(dfs, axis=0)
            
            print('quote_data for '+key+ ' has been processed')
    return dfs
        

with open('../data/market_order_json/gemini-btc-usd-spot_2024_1.json') as f:
    data_quote = json.load(f)
dfs = json_to_df(data_quote)

