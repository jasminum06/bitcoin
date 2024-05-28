'''
This file is for calculating market depth
depth: average number of amounts of inside ask and inside bid
'''

# libraries
import pandas as pd
import glob

# function
def cal_market_depth(market_name, start_date, end_date):
    
    quote_paths = glob.glob('../data/market_order_level1_processed/'+market_name+('*.csv'))
    quote_data_list = []
    for quote_path in quote_paths:
        quote_data_list.append(pd.read_csv(quote_path)[['time', 'ask_size', 'bid_size']])
    quote_data = pd.concat(quote_data_list, axis = 0)
    
    quote_data = quote_data.set_index('time')
    quote_data.index = pd.to_datetime(quote_data.index)
    quote_data = quote_data[(quote_data.index>=pd.to_datetime(start_date))&
                            (quote_data.index<=pd.to_datetime(end_date))]
    market_depth = (quote_data['ask_size'].astype(float) + quote_data['bid_size'].astype(float))/2
    market_depth = pd.DataFrame(market_depth)
    market_depth.columns = ['market_depth']
    print(market_depth.head())
    market_depth = market_depth.resample('D').mean()
    market_depth.to_csv('../result/'+market_name+'/market_depth.csv')
    print('market depth for '+market_name+' has been saved')    
    

    
start_date = '20231101'
end_date = '20240430'
market_names = ['bitstamp-btc-usd', 'gemini-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd']
for market_name in market_names:
    cal_market_depth(market_name, start_date, end_date)