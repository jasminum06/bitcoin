import pandas as pd

def describe_data(data, start_date=None, end_date = None):
    
    if 'time' in data.columns:
        data = data.set_index('time')
    data.index = pd.to_datetime(data.index)
    if start_date is not None:
        data = data[data.index>=pd.to_datetime(start_date)]
    if end_date is not None:
        data = data[data.index<=pd.to_datetime(end_date)]
    
    return data.mean()[0], data.median()[0], data.std()[0]

def excel_data(data):
    data_mean1, data_mid1, data_std1 = describe_data(data, 
                                                  start_date = '20231101', end_date = '20240110')
    data_mean2, data_mid2, data_std2 = describe_data(data, start_date = '20240111')
    df = pd.DataFrame([[data_mean1, data_mean2], [data_mid1, data_mid2], [data_std1, data_std2]],
                 index = ['Mean', 'Median', 'Std.dev.'],
                 columns = ['2023/11-2024/1/10', '2024/1/11-'])
    df['market'] = market_name
    return df
    


market_names = ['bitstamp-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd', 'gemini-btc-usd']
size_df = pd.DataFrame()
counts_df = pd.DataFrame()
large_trade_df = pd.DataFrame()
small_trade_df = pd.DataFrame()
espread_df = pd.DataFrame()
for market_name in market_names:
    size_data = pd.read_csv('../result/'+market_name+'/spot_mean_trade_size.csv')
    size_df = pd.concat([excel_data(size_data),size_df],axis = 0)
    counts_data = pd.read_csv('../result/'+market_name+'/spot_trade_counts.csv')
    counts_df = pd.concat([counts_df, excel_data(counts_data)],axis =0)
    
    large_data = pd.read_csv('../result/'+market_name+'/spot_trade_percentage.csv')[['time', 'percentage_large']]
    large_trade_df = pd.concat([large_trade_df, excel_data(large_data)],axis=0)
    small_data = pd.read_csv('../result/'+market_name+'/spot_trade_percentage.csv')[['time', 'percentage_small']]
    small_trade_df = pd.concat([small_trade_df,excel_data(small_data)],axis=0)
    
    espread = pd.read_csv('../result/'+market_name+'/'+market_name+'_espread.csv')
    espread_df = pd.concat([espread_df, excel_data(espread)],axis=0)
    
    
    
size_df.to_csv('../result/description_all/size_decription.csv')
counts_df.to_csv('../result/description_all/counts_decription.csv')
large_trade_df.to_csv('../result/description_all/large_trade_decription.csv')
small_trade_df.to_csv('../result/description_all/small_trade_decription.csv')
espread_df.to_csv('../result/description_all/espread_decription.csv')
    
    
    
    
                     

    
