import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import glob
import json
import os

'''plot template'''
# plot template
def plot_axis(plot_info:dict, title, outputdir, file_type='png', fontsize=20):
    # plot_info: number of keys should not exceed 2
    fig, ax1 = plt.subplots(figsize=(16,12))
    n_figures = len(plot_info.keys())
    for i in range(n_figures):
        if i == 0:
            ax = ax1
        else:
            ax = ax1.twinx()
        key = list(plot_info.keys())[i]
        X = plot_info[key]["X"]
        Y = plot_info[key]["Y"]
        plot_type = plot_info[key]["type"]
        if plot_type == "bar":
            ax.bar(X, Y, color='orange', label=plot_info[key]["label"])
        elif plot_type == "line":
            ax.plot(X, Y, color='royalblue', label=plot_info[key]["label"])
        ax.grid(True)
        ax.set_ylabel(plot_info[key]["ylabel"], fontdict={"size":fontsize})
        ax.legend(plot_info[key]["legend"], loc='upper left', fontsize=fontsize)
        if "xticks" in plot_info[key]:
            ax.set_xticks(plot_info[key]["xticks"])
        if "xticklabels" in plot_info[key]:
            ax.set_xticklabels(plot_info[key]["xticklabels"] , rotation=90, fontsize=fontsize)
        if "axvline" in plot_info[key]:
            ax.axvline(plot_info[key]["axvline"], color='red', linestyle='--',linewidth = 1)
        ax.yaxis.set_tick_params(labelsize=fontsize)
    plt.tight_layout(pad=2.0)
    plt.title(title, fontsize=fontsize)
    fig.savefig(
        outputdir + title + "." + file_type,
    )
    plt.close()

''' effective spread'''
def process_spot_data(spot_data:pd.DataFrame, start_date = None):
    if 'time' in spot_data.columns:
        spot_data = spot_data.set_index('time')
    spot_data.index = pd.to_datetime(spot_data.index)
    if spot_data.index.tz is not None:
        spot_data.index = spot_data.index.tz_localize(None)
        
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        spot_data = spot_data[spot_data.index>=start_date]
        
    return spot_data

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

    
def plot_espread(espread:pd.DataFrame, mark_date:str, market_name: str, start_date:str):
    # plot effective spread
    espread = espread[espread.index>=pd.to_datetime(start_date)]
    dates = espread.index
    espreads = espread['espread'].values*10000
    plot_info = {
        "1":{
        "X":dates,
        "Y":espreads,
        "type":"line",
        "label":"espread(bps)",
        "ylabel":"espread(bps)",
        "legend":['espread(bps)'],
        "xticks":dates[::5],
        "xticklabels":[
            dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
        ],
        "axvline":pd.to_datetime(mark_date)}
    }
    title = 'espread(bps) for' + market_name
    outputdir = "../figures/espread/"
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    
    plot_axis(plot_info, title, outputdir, file_type='png', fontsize=20)
    
    
# final function: calculate & plot
def cal_n_plot(market_name:str, order_level:str, mark_date:str, start_date:str):
    # spot data
    market_spot = pd.read_csv('../data/spot/'+market_name+'-spot_spot.csv')
    market_spot = process_spot_data(market_spot)
    def month_format(month_val:str):
        year, month = month_val.split('_')
        month = str(int(month))
        return year+'_'+month
    market_spot['month'] =  market_spot.index.strftime('%Y_%m')
    market_spot['month'] = market_spot['month'].apply(month_format)
    market_spot['date'] =  market_spot.index.strftime('%Y-%m-%d')
    month_list = market_spot['month'].unique()
    # quote data
    # market_quote_paths = glob.glob('../data/market_order_json/'+market_name+'-spot_*.json')
    
    # every month match
    def everymonth_espread(month_list, spot_data, order_level):
        espread_dfs = []
        for month in month_list:
            month_spot = spot_data[spot_data['month'] ==month]
            if os.path.exists('../data/market_order_json/'+market_name+'-spot_'+month+'.json'):
                with open('../data/market_order_json/'+market_name+'-spot_'+month+'.json') as f:
                    data_quote = json.load(f)
            else:
                continue
            date_quote_dict = json_to_df(data_quote)
            for date in month_spot['date'].unique():
                date_spot = month_spot[month_spot['date'] == date]
                date_quote = date_quote_dict[date]
                date_merge = match_spot_quote(date_spot,date_quote , order_level)
                date_espread = cal_espread(date_merge)  
                espread_dfs.append(date_espread)
            
        print('espread for '+month+' has been calculated')
        final_espread = pd.concat(espread_dfs, axis = 0)
        
        return final_espread
    
    market_espread = everymonth_espread(month_list, market_spot, order_level)
    # save result
    if not os.path.exists('../result/'+market_name+'/'):
        os.makedirs('../result/'+market_name+'/')
    market_espread.to_csv('../result/'+market_name+'/espread.csv')

    # plot
    plot_espread(market_espread, mark_date, market_name = market_name, start_date = start_date)

start_date = '20231101'
mark_date = '20240111'
market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']
for market_name in market_names:
    cal_n_plot(market_name, order_level ='level_1', mark_date = mark_date, start_date = start_date)
    print('result for '+market_name+' has been saved')









