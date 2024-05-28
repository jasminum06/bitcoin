# -*- coding: utf-8 -*-
"""
Created on Thu May 16 15:52:22 2024

@author: lynie
"""
import pandas as pd

market_names = ['bitstamp-btc-usd', 'gemini-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd']
for market_name in market_names:
    spot_path = '../data/spot/'+market_name+'-spot_spot.csv'
    spot_data = pd.read_csv(spot_path).set_index('time')
    spot_data = spot_data.drop('time', axis=0)
    
    spot_data.to_csv('../data/spot/'+market_name+'-spot_spot_2.csv')
    print(market_name + ' has been saved')