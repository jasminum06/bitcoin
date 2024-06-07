import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import glob
import json

''' effective spread'''
def json_to_df(quote_data:dict):
    # result: same as csv: columns = time,market,level,coin_metrics_id,database_time,
    # ask_price,ask_size,bid_price,bid_size
    # convert to pd.DataFrame:
    del quote_data['market'],quote_data['ask'],quote_data['bid']
    quote_data_dict = {}
    for key, value in quote_data.items():
       
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
        if len(dfs) == 0:
            quote_data_dict[key] = dfs
            print('market order data is null on '+ key)
        else:
            quote_data_daily = pd.concat(dfs, axis=0)
            quote_data_daily = quote_data_daily[['time','market','level','coin_metrics_id','database_time',
            'ask_price','ask_size','bid_price','bid_size']]
            quote_data_dict[key] = quote_data_daily
        
    return quote_data_dict

def cal_bas(market_name:str, level = 'level_1'):
    
    quote_paths = glob.glob('../data/market_order_json/'+market_name+'-spot_*.json')
    bas_all = pd.DataFrame()
    for quote_path in quote_paths:
        with open(quote_path) as f:
            data_quote = json.load(f)
        quote_data_dict = json_to_df(data_quote)
        for key in quote_data_dict.keys():
            quote_data_df = quote_data_dict[key]
            if len(quote_data_df) == 0:
                continue
            else:
                quote_data_df = quote_data_df.set_index('time')
                quote_data_df.index = pd.to_datetime(quote_data_df.index)
                quote_data_df['bid_ask_spread'] = quote_data_df['bid_price'] - quote_data_df['ask_price']
                quote_data_df_level = quote_data_df[quote_data_df['level'] == 'level_1']
                bas_all = pd.concat([bas_all,quote_data_df_level[['bid_ask_spread']].resample('D').mean()],axis =0)
            
    bas_all.to_csv('../result/'+market_name+'/bid_ask_spread.csv')
    print('bid-ask-spread has been calculated for '+ market_name)
    
market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']
for market_name in market_names:
    cal_bas(market_name)
    
