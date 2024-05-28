# libararies
import pandas as pd
from scipy.stats import norm
from datetime import datetime
import numpy as np
import csv
import os
import json

# BVC algorithm
# Level-1 classification
def volume_buckets(data:pd.DataFrame, V):
    
    data['side'] = data['side'].replace(0,-1)
    # step 1- divide into bars by volume
    interval_index = []
    current_sum = 0
    start_index = 0
    last_bar_amount = []
    first_bar_amount = [data.iloc[0]['amount']]
    for i, row in data.iterrows():
        current_sum = current_sum + row['amount']
        if current_sum == V: # if the current sum is exactly equal to V
            interval_index.append((start_index, i))
            start_index = i + 1
            current_sum = 0
            last_bar_amount.append(row['amount'])
            if i < len(data)-1:
                first_bar_amount.append(data.iloc[i+1]['amount'])
        elif current_sum > V: # if the current sum is larger than V
            interval_index.append((start_index, i))
            start_index = i
            last_bar_amount.append(row['amount'] - (current_sum-V))
            current_sum = current_sum-V
            while current_sum > V:
                interval_index.append((i, i))
                last_bar_amount.append(V)
                first_bar_amount.append(V)
                current_sum = current_sum-V
                
            first_bar_amount.append(current_sum)
            
            
    # deal with the last interval
    if start_index < len(data):
        interval_index.append((start_index, len(data) - 1))
        last_bar_amount.append(data.iloc[-1]['amount'])
        
    # divide the data frame
    interval_dfs = []
    for start, end in interval_index:
        interval_dfs.append(data.iloc[start:end + 1])
    # deal with the first and last values in each bar
    for i in range(len(interval_dfs)):
        
        interval_df = interval_dfs[i]
        interval_df.loc[interval_df.index[0], 'amount'] = first_bar_amount[i]
        interval_df.loc[interval_df.index[-1], 'amount']= last_bar_amount[i]
        interval_df['price'] = interval_df['price'].astype(float)
        interval_dfs[i] = interval_df.set_index('time')
        
    return interval_dfs

# BVC classification
def BVC_classification(interval_dfs: list):
    V_B_tau = []
    V_S_tau = []
    P_begin = interval_dfs[0]['price'].iloc[0]
    for interval_df in interval_dfs:
        V_B_i_lst = []
        #time bar
        for group in interval_df.groupby(pd.Grouper(freq='1min')):
            dt_data = group[1]
            V_i = dt_data['amount'].sum()
            if len(dt_data)>1:
                P_end = dt_data['price'].iloc[-1]
                dt_sigma = dt_data['price'].std()
                if dt_sigma == 0:
                    V_B_i = 0.5*V_i
                else:
                    V_B_i = V_i*(norm.cdf((P_end-P_begin)/dt_sigma, 0, 1))
            elif len(dt_data) ==1:
                V_B_i = 0.5*V_i # TODO ELO definition or CPS definition? Page 58
                
            V_B_i_lst.append(V_B_i)
            P_begin = P_end
        
        V_B_tau.append(sum(V_B_i_lst))
        V_S_tau.append(interval_df['amount'].sum()-sum(V_B_i_lst))
                
    return V_B_tau, V_S_tau

def cal_OI(V_B_tau, V_S_tau):
    
    return np.abs([x - y for x, y in zip(V_B_tau, V_S_tau)])
    
def cal_VPIN(OI_tau, V, N):
    VPIN_lst = []
    for i in range(len(OI_tau)-N):
        VPIN_lst.append(sum(OI_tau[i:i+N]/V)/N)
    return VPIN_lst

# real classification

def Real_classification(interval_dfs:list):
    
    V_B_tau = []
    V_S_tau = []
    for interval_df in interval_dfs:
        V_B_tau.append(interval_df[interval_df['side'] == 1]['amount'].sum())
        V_S_tau.append(interval_df[interval_df['side'] == -1]['amount'].sum())
    
    return V_B_tau, V_S_tau

# Lee-Ready algorithm
# Level-2 classification
# classifiy buyer-initated and seller-initated volume
# trade by trade
# Level-2 classsification

# quote data & spot data
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
    print('quote_data for '+key+ ' has been processed')
        
    return quote_data_dict

# match quote data and spot data    
def match_spot_quote(spot_data:pd.DataFrame, quote_data:pd.DataFrame, order_level = 'level_1'):
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
        
        # merged_data: mid-quote, price
        merged_data['LR_side'] = np.where(merged_data['mid_quote'].values <merged_data['price'].values, 1, 
                                np.where(merged_data['mid_quote'].values > merged_data['price'].values, -1, 
                                         np.nan))
        
        merged_data['LR_side'] = merged_data['LR_side'].fillna(method = 'ffill')
        
    return merged_data


def LR_classification(interval_dfs:list):

    V_B_tau = []
    V_S_tau = []
    for interval_df in interval_dfs:
        V_B_tau.append(interval_df[interval_df['LR_side' ]== 1]['amount'].sum())
        V_S_tau.append(interval_df[interval_df['LR_side'] == -1]['amount'].sum())
        
    return V_B_tau, V_S_tau

def LR_VPIN(spot_data, quote_data, V, N):
    merged_data = match_spot_quote(spot_data, quote_data, order_level = 'level_1')
    interval_dfs = volume_buckets(merged_data, V)
    V_B_tau, V_S_tau = BVC_classification(interval_dfs)
    OI_tau = cal_OI(V_B_tau, V_S_tau)
    VPIN_lst = cal_VPIN(OI_tau, V, N)
    return VPIN_lst




# classification accuracy
def classify_accuracy(V_B_tau_est, V_S_tau_est, V_B_tau, V_S_tau):
    
    accuracy_tau = [(min(x_est, x)+min(y_est,y))/(x+y) for x_est, y_est, x, y in zip(V_B_tau_est, V_S_tau_est, V_B_tau, V_S_tau)]
            
    return accuracy_tau

# results
k = 50
N = 50
start_date = '20231101'
end_date = '20240430'
market_names = ['bitstamp-btc-usd', 'gemini-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd']
for market_name in market_names:
    merged_data = pd.read_csv('../data/merge_spot_midquote/'+market_name+'-spot_midquote.csv')
    merged_data = merged_data.set_index('time')
    merged_data.index = pd.to_datetime(merged_data.index)
    merged_data = merged_data[(merged_data.index>=pd.to_datetime(start_date)) & (merged_data.index<=pd.to_datetime(end_date))]
    V = ((merged_data[['amount']].resample('D').sum()).mean()/k)[0]
    merged_data = merged_data.reset_index()
    
    interval_dfs = volume_buckets(merged_data, V)
    V_B_real, V_S_real = Real_classification(interval_dfs)
    V_B_LR, V_S_LR = LR_classification(interval_dfs)
    V_B_BVC, V_S_BVC = BVC_classification(interval_dfs)
    
    bvc_accuracy = np.mean(classify_accuracy(V_B_BVC, V_S_BVC, V_B_real, V_S_real))
    lr_accuracy = np.mean(classify_accuracy(V_B_LR, V_S_LR, V_B_real, V_S_real))
    print('LR accuracy for '+market_name +' is ' + str(lr_accuracy)+
          ', BVC accuracy is '+ str(bvc_accuracy))
    
    VPIN_real = cal_VPIN(cal_OI(V_B_real, V_S_real),V, N)
    VPIN_lr = cal_VPIN(cal_OI(V_B_LR, V_S_LR), V, N)
    VPIN_bvc = cal_VPIN(cal_OI(V_B_BVC, V_S_BVC), V, N)
    VPIN_df = pd.DataFrame()
    VPIN_df['VPIN_real'] = VPIN_real
    VPIN_df['VPIN_LR'] = VPIN_lr
    VPIN_df['VPIN_BVC'] = VPIN_bvc
    
    file_path = '../result/'+market_name+'/VPIN.csv'
    VPIN_df.to_csv(file_path)
    print('VPIN for '+market_name+' has been saved')
        
    

# =============================================================================
# # calculate VPIN
# # parameter settings
# N = 50
# k = 50 # determine V
# start_date = '20231101'
# end_date = '20240430'
# market_names = ['bitstamp-btc-usd', 'gemini-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd']
# for market_name in market_names:
#     spot_path = '../data/spot/'+market_name+'-spot_spot.csv'
#     spot_data = pd.read_csv(spot_path).set_index('time')
#     spot_data.index = pd.to_datetime(spot_data.index)
#     if spot_data.index.tz is not None:
#         spot_data.index = spot_data.index.tz_localize(None)
#     spot_data = spot_data[(spot_data.index>=pd.to_datetime(start_date)) & (spot_data.index<=pd.to_datetime(end_date))]
#     spot_data['amount'] = spot_data['amount'].astype('float')
#     
#     V = ((spot_data[['amount']].resample('D').sum()).mean()/k)[0] # average daily amount
#     spot_data = spot_data.reset_index('time')
#     
#     interval_dfs = volume_buckets(spot_data, V)
#     V_B_real, V_S_real = Real_classification(interval_dfs)
#     V_B_BVC, V_S_BVC = BVC_classification(interval_dfs)
#     bvc_accuracy = np.mean(classify_accuracy(V_B_BVC, V_S_BVC, V_B_real, V_S_real))
#     print('BVC accuracy for '+market_name +' is ' + str(bvc_accuracy))
#     
#     VPIN_real = cal_VPIN(cal_OI(V_B_real, V_S_real),V, N)
#     VPIN_bvc = cal_VPIN(cal_OI(V_B_BVC, V_S_BVC), V, N)
#     
#     VPIN_df = pd.DataFrame()
#     VPIN_df['VPIN_real'] = VPIN_real
#     VPIN_df['VPIN_BVC'] = VPIN_bvc
#     
#     file_path = '../result/'+market_name+'/VPIN.csv'
#     VPIN_df.to_csv(file_path)
#     print('BVC VPIN for '+market_name+' has been saved')
# =============================================================================


# LR calculation
# =============================================================================
# start_date = '20231101'
# end_date = '20240501'
# month_list = ['2023_11', '2023_12', '2024_1', '2024_2', '2024_3', '2024_4']
# order_level = 'level_1'
# k = 50
# N =50
# market_names = ['bitstamp-btc-usd', 'gemini-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd']
# for market_name in market_names:
#     spot_path = '../data/spot/'+market_name+'-spot_spot.csv'
#     spot_data = pd.read_csv(spot_path).set_index('time')
#     spot_data.index = pd.to_datetime(spot_data.index)
#     if spot_data.index.tz is not None:
#         spot_data.index = spot_data.index.tz_localize(None)
#     spot_data = spot_data[(spot_data.index>=pd.to_datetime(start_date)) & (spot_data.index<=pd.to_datetime(end_date))]
#     spot_data['amount'] = spot_data['amount'].astype('float')
#     V = ((spot_data[['amount']].resample('D').sum()).mean()/k)[0] # average daily amount
#     
#     def month_format(month_val:str):
#         year, month = month_val.split('_')
#         month = str(int(month))
#         return year+'_'+month
#     spot_data['month'] =  spot_data.index.strftime('%Y_%m')
#     spot_data['month'] = spot_data['month'].apply(month_format)
#     spot_data['date'] = spot_data.index.strftime('%Y-%m-%d')
#     
#     
#     date_merge_data = []
#     for month in month_list:
#         month_spot = spot_data[spot_data['month'] ==month]
#         if os.path.exists('../data/market_order_json/'+market_name+'-spot_'+month+'.json'):
#             with open('../data/market_order_json/'+market_name+'-spot_'+month+'.json') as f:
#                 data_quote = json.load(f)
#         else:
#             continue
#         date_quote_dict = json_to_df(data_quote)
#         for date in month_spot['date'].unique():
#             date_spot = month_spot[month_spot['date'] == date]
#             date_quote = date_quote_dict[date]
#             date_merge = match_spot_quote(date_spot,date_quote , order_level)
#             date_merge_data.append(date_merge)
#     
#     merged_data = pd.concat(date_merge_data, axis = 0)
#     if not os.path.exists('../data/merge_spot_midquote/'):
#         os.makedirs('../data/merge_spot_midquote/')
#     merged_data.to_csv('../data/merge_spot_midquote/'+market_name+'-spot_midquote.csv')
#     merged_data = merged_data.reset_index()
#     interval_dfs = volume_buckets(merged_data, V)
#     V_B_real, V_S_real = Real_classification(interval_dfs)
#     V_B_LR, V_S_LR = LR_classification(interval_dfs)
#     
#     lr_accuracy = np.mean(classify_accuracy(V_B_LR, V_S_LR, V_B_real, V_S_real))
#     print('LR accuracy for '+market_name +' is ' + str(lr_accuracy))
#     
#     VPIN_real = cal_VPIN(cal_OI(V_B_real, V_S_real),V, N)
#     VPIN_lr = cal_VPIN(cal_OI(V_B_LR, V_S_LR), V, N)
#     
#     VPIN_df = pd.DataFrame()
#     VPIN_df['VPIN_real'] = VPIN_real
#     VPIN_df['VPIN_LR'] = VPIN_lr
#     
#     file_path = '../result/'+market_name+'/VPIN_LR.csv'
#     VPIN_df.to_csv(file_path)
#     print('LR VPIN for '+market_name+' has been saved')
# 
# =============================================================================






# TODO
# LR和real的VPIN计算， VPIN时间戳的对应
# market order csv数据bitstamp-btc-usd检查


    
    






