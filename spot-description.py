import pandas as pd
import datetime as dt
import os

# read data and process
def process_data(market_name):
    data = pd.read_csv('../data/spot/'+market_name+'-spot_spot.csv')
    if 'time' in data.columns:
        data = data.set_index('time')
    data.index = pd.to_datetime(data.index)
    if data.index.tz is not None:
        data.index = data.index.tz_localize(None)
    return data


# fig2 - panel A: trade size
def cal_trade_size(data, market_name,start_date = None, amount_type = 'mean'):
    
    if start_date is not None:
        data = data[data.index>=pd.to_datetime(start_date)]
    if amount_type == 'mean':
        trade_size = data[['amount']].resample('D').mean()
    elif amount_type == 'sum':
        trade_size = data[['amount']].resample('D').sum()
    elif amount_type == 'median':
        trade_size = data[['amount']].resample('D').median()
    if not os.path.exists('../result/'+market_name+'/'):
        os.makedirs('../result/'+market_name+'/')
    trade_size.to_csv('../result/'+market_name+'/spot_'+ amount_type + '_trade_size.csv')
    print('trdae size for '+market_name+' has been saved')
    

# fig2 - panel B: number of trades
def cal_trade_counts(data, market_name,start_date = None):
    
    if start_date is not None:
        data = data[data.index>=pd.to_datetime(start_date)]
    trade_counts = data[['amount']].resample('D').count()
    trade_counts.columns = ['count']
    if not os.path.exists('../result/'+market_name+'/'):
        os.makedirs('../result/'+market_name+'/')
        
    trade_counts.to_csv('../result/'+market_name+'/spot_trade_counts.csv')
    print('trdae counts for '+market_name+' has been saved')

# fig3 - panel A and B: percentage of trades
def cal_trade_percentage(data, market_name, start_date=None, threshold = 'mean'):
    if start_date is not None:
        data = data[data.index>=pd.to_datetime(start_date)]    
    def percentage(group, threshold):
        large_percentage = (group[group['amount']>=threshold][['amount']].count()/group['amount'].count()).values
        small_percentage = (group[group['amount']<threshold][['amount']].count()/group['amount'].count()).values
        return pd.Series({'percentage_large':large_percentage , 'percentage_small':small_percentage})    
   
    if threshold == 'mean':
        threshold = data['amount'].mean()
    threshold = data['amount'].mean()
    def array_to_float(arr):
        return float(arr[0])
    trade_percentage = data.resample('D').apply(percentage,threshold=threshold)
    trade_percentage['percentage_large'] = trade_percentage['percentage_large'].apply(array_to_float)
    trade_percentage['percentage_small'] = trade_percentage['percentage_small'].apply(array_to_float)
    
    if not os.path.exists('../result/'+market_name+'/'):
        os.makedirs('../result/'+market_name+'/')
    trade_percentage.to_csv('../result/'+market_name+'/spot_trade_percentage.csv')
    print('trdae percentage for '+market_name+' has been saved')
    
    
# calculation & save
market_names =  ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']
for market_name in market_names:
    data = process_data(market_name)
    cal_trade_size(data, market_name,start_date = None, amount_type = 'mean')
    cal_trade_counts(data, market_name, start_date = None)
    cal_trade_percentage(data, market_name, start_date=None, threshold = 'mean')
    print('trade size, counts and percentage for '+market_name+' has benn saved')
    
    

    
        
        
        

    
    
    

