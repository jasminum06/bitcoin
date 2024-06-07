'''
This file is for calculating the realized spread and adverse selection.
We estimate revenue to liquidity providers using the 5-minute realized spread, 
which assumes the liquidity provider is able to close her position at the quote 
midpoint 5 minutes after the trade.
We measure gross losses to liquidity demanders due to adverse selection using 
the 5-minute price impact of a trade.
'''

# libraries
import pandas as pd
import json
import os
import glob

# function
def cal_mid_quotes(market_name):
    
    quote_paths = glob.glob('../data/market_order_level1_processed/'+market_name+('*.csv'))
    quote_data_list = []
    for quote_path in quote_paths:
        quote_data_list.append(pd.read_csv(quote_path))
        
    return pd.concat(quote_data_list, axis = 0)
    
def cal_merged_data(market_name, mid_quotes, start_date, end_date):
    
    merged_data = pd.read_csv('../data/merge_spot_midquote/'+market_name+'-spot_midquote.csv')
    ''' merged_data.columns = ['time', 'side', 'mid_quote', 'price']'''
    merged_data = merged_data.set_index('time')
    merged_data.index = pd.to_datetime(merged_data.index)
    if merged_data.index.tz is not None:
        merged_data.index = merged_data.index.tz_localize(None)
    # 
    # mid_quotes = merged_data[['mid_quote']]
    # mid_quotes.columns = ['mid_quote_5min']
    # mid_quotes = mid_quotes.reset_index()
    # mid_quotes['time'] = mid_quotes['time'] - pd.Timedelta(minutes=5)
    mid_quotes = mid_quotes[['time','mid_quote']]
    mid_quotes.columns = ['time','mid_quote_5min']
    mid_quotes.loc[:,'time'] = pd.to_datetime(mid_quotes['time'])
    mid_quotes.loc[:,'time'] = mid_quotes['time'] - pd.Timedelta(minutes=5)
    
    merged_data = merged_data.sort_values(by='time')
    mid_quotes = mid_quotes.sort_values(by='time')
    merged_data = pd.merge_asof(merged_data[['price', 'mid_quote', 'side','amount']].reset_index(), 
                                 mid_quotes, on = 'time', direction = 'forward')
    merged_data = merged_data[(merged_data['time'] >=pd.to_datetime(start_date)) & 
                              (merged_data['time'] <= pd.to_datetime(end_date))]
    
    return merged_data.set_index('time')
    

def cal_spreads(merged_data):
    
    def cal_rspread(x):
        return x['side']*(x['price']-x['mid_quote_5min'])/x['mid_quote']
    
    def cal_adv_selection(x):
        return x['side']*(x['mid_quote_5min'] - x['mid_quote'])/x['mid_quote']
    
    merged_data['rspread'] = pd.DataFrame(cal_rspread(merged_data))
    merged_data['adv_selection'] = pd.DataFrame(cal_adv_selection(merged_data))
    
    def amount_weighted_average(group, key):
        weighted_spread = (group[key] * group['amount']).sum() / group['amount'].sum()
        return pd.Series({key: weighted_spread})
    rspread = merged_data.resample('D').apply(amount_weighted_average, key = 'rspread')
    adv_selection = merged_data.resample('D').apply(amount_weighted_average, key = 'adv_selection')
    
    return rspread, adv_selection

start_date = '20231101'
end_date = '20240430'
market_names = ['bitstamp-btc-usd', 'gemini-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd']
for market_name in market_names:
    mid_quotes = cal_mid_quotes(market_name)
    merged_data = cal_merged_data(market_name, mid_quotes, start_date, end_date)
    rspread, adv_selection = cal_spreads(merged_data)
    rspread.to_csv('../result/'+market_name+'/realized_spread.csv')
    adv_selection.to_csv('../result/'+market_name+'/adverse_selection.csv')
    print('caculation for ' + market_name+' is done')
    
    #TODO
    # check sum == espread?


    