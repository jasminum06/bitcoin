import pandas as pd
import numpy as np
import os
import json
import glob

def price_weighted_quotes(market_name, quote_data, num_level):
    
    level_list = ['level_{}'.format(i) for i in range(num_level)]
    filtered_quotes = quote_data[quote_data['level'].isin(level_list)]
    
    ask_prices = filtered_quotes.pivot(index = 'time', columns = 'level', values = 'ask')
    bid_prices = filtered_quotes.pivot(index = 'time', columns = 'level', values = 'bid')
    
    def weighted_avg(row):
        values = row.values
        total = values.sum()
        weights = values / total
        weighted_sum = (values * weights).sum()
        return weighted_sum
        
    ask_prices['weighted_ask'] = ask_prices.apply(weighted_avg, axis = 1)
    bid_prices['weighted_bid'] = bid_prices.apply(weighted_avg, axis = 1)
    
    weighted_quotes = pd.concat([ask_prices[['weighted_ask']], bid_prices[['weighted_bid']]], axis = 1)
    weighted_quotes['weighted_mid'] = weighted_quotes.mean(axis = 1).values
    
    if not os.path.exists('../result/data/weighted_quotes/'):
        os.makedirs('../result/data/weighted_quotes/')
    weighted_quotes.to_csv('../result/data/weighted_quotes/'+market_name+'-weighted_quotes_'+str(num_level)+'_levels.csv')
    
    return weighted_quotes
    


