# check espread- bitstamp Jan 9th
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import glob
import json
import os

# market_order_data
market_order = pd.read_csv('../data/market_order/bitstamp-btc-usd-spot/market_order_2024-01-09.csv')
market_order2 = pd.read_csv('../data/market_order/bitstamp-btc-usd-spot/market_order_2024-01-08.csv')
# spot_data
spot = pd.read_csv('../data/spot/bitstamp-btc-usd-spot_spot.csv')
# process spot data
def process_spot_data(spot_data:pd.DataFrame, start_date = None,end_date = None):
    if 'time' in spot_data.columns:
        spot_data = spot_data.set_index('time')
    spot_data.index = pd.to_datetime(spot_data.index)
    if spot_data.index.tz is not None:
        spot_data.index = spot_data.index.tz_localize(None)
        
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        spot_data = spot_data[spot_data.index>=start_date]
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        spot_data = spot_data[spot_data.index<end_date]
        
    return spot_data
# match time stamp
def match_spot_quote(spot_data:pd.DataFrame, quote_data:pd.DataFrame, order_level:str):
    # match the time stamp for spot data and quotes
    # spot_data: processed, time, amount, price, side
    # quote_data: market order data, 10s
    if len(quote_data) == 0:
        merged_data = pd.DataFrame()
    else:
        quote_data = quote_data[quote_data['level'] == order_level]
        if 'time' in quote_data.columns:
            quote_data = quote_data.set_index('time')
        quote_data.index = pd.to_datetime(quote_data.index)
        if quote_data.index.tz is not None:
            quote_data.index = quote_data.index.tz_localize(None)
        
        quote_data['mid_quote'] = (quote_data['ask_price']+quote_data['bid_price'])/2 # mid-quotes
        merged_data = pd.merge_asof(spot_data[['amount', 'price', 'side']], quote_data[['mid_quote']], 
                                    left_index=True, right_index=True, direction='backward') # match timestamp (earlier and nearest)
        merged_data['side'] = merged_data['side'].replace(0,-1) # buy side ==1, sell side == -1
    
    return merged_data

def cal_espread(merged_data:pd.DataFrame, start_date = None, daily = True):
    # calculate effective spread
    if merged_data.empty:
        espread = pd.DataFrame()
    else:
        merged_data['espread'] = merged_data['side']*(merged_data['price'] - merged_data['mid_quote'])/merged_data['mid_quote']
        
        if daily == True:
            def amount_weighted_average(group):
                weighted_spread = (group['espread'] * group['amount']).sum() / group['amount'].sum()
                return pd.Series({'espread': weighted_spread})
            espread = merged_data.resample('D').apply(amount_weighted_average)
            espread = pd.DataFrame(espread)
        else:
            espread = merged_data[['espread']]
        
        if start_date is not None:
            start_date = pd.to_datetime(start_date)
            espread = espread[espread.index>=start_date]
        
    return espread

start_date = '20240109'
end_date = '20240110'
spot1 = process_spot_data(spot, start_date, end_date)
merged_data = match_spot_quote(spot1, market_order, order_level = 'level_1')
espread = cal_espread(merged_data, start_date, daily = False)


spot2 = process_spot_data(spot, '20240108','20240109')
merged_data2 = match_spot_quote(spot2, market_order2, order_level = 'level_1')
espread2 = cal_espread(merged_data2, '20240108', daily = False)

