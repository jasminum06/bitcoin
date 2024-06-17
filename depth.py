import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import os
from pathlib import Path
import datetime as dt


# internal import
from utils import (process_spot_data, 
                   month_format, 
                   plot_axis)
    
# data processing
class Depth:
    def __init__(self, start_date, mark_date, level_number:int, freq:str):
        self.start_date = start_date
        self.mark_date = mark_date
        self.level_number = level_number # level_number >=20
        self.freq = freq # freq = 'tick' or 'daily'
        
        
    def json_to_dfs(self, quote_data:dict):
        # result: same as csv: columns = time,market,level,coin_metrics_id,database_time,
        # ask_price,ask_size,bid_price,bid_size
        # convert to pd.DataFrame:
        del quote_data['market'],quote_data['ask'],quote_data['bid']
        quote_data_dict = {}
        
        for date in quote_data:
            mid_quotes = []
            for i in range(self.level_number):
                midquote_temp = [{'time': item['time'], 
                                'mid_quote': (float(item['asks'][i]['price']) + float(item['bids'][i]['price']))/2,
                                'ask_price': float(item['asks'][i]['price']),
                                'ask_size': float(item['asks'][i]['size']),         
                                'bid_price': float(item['bids'][i]['price']),
                                'bid_size': float(item['bids'][i]['size']),
                                'level': 'level_{}'.format(str(i+1))
                                }
                            for item in quote_data[date]]
                mid_quotes.extend(midquote_temp)

            mid_quotes = pd.DataFrame(mid_quotes)
            mid_quotes['date'] = mid_quotes['time'].apply(lambda x: x[:10])
            mid_quotes['time'] = mid_quotes['time'].apply(lambda x: x[11:19])

            mid_quotes['timestamp'] = pd.to_datetime(mid_quotes['date'].astype(str) + ' ' + mid_quotes['time'])
            mid_quotes = mid_quotes[['timestamp', 'ask_price', 'ask_size','bid_price', 'bid_size','level', 'mid_quote']]
            mid_quotes.columns = ['time', 'ask_price', 'ask_size','bid_price', 'bid_size','level', 'mid_quote']
        
            quote_data_dict[date] = mid_quotes.sort_values(by = 'time')
            
        print('quote_data for '+date+ ' has been processed')
        
        return quote_data_dict

    def load_from_json(self, json_file):
        if os.path.exists(json_file):
            with open(json_file) as f:
                data_quote = json.load(f)
            return self.json_to_dfs(data_quote)
        else:
            return None
    
    def cal_quote_depth(self, quote_data):
        # level_list = ['level_{}'.format(i) for i in range(self.level_number)]
        # filtered_quotes = quote_data[quote_data['level'].isin(level_list)]
        
        quote_data = quote_data.sort_values(by =['time','level'])
        ask_prices = quote_data.pivot(index = 'time', columns = 'level', values = 'ask_price')
        bid_prices = quote_data.pivot(index = 'time', columns = 'level', values = 'bid_price')
        ask_sizes = quote_data.pivot(index = 'time', columns = 'level', values = 'ask_size')
        bid_sizes = quote_data.pivot(index = 'time', columns = 'level', values = 'bid_size')
        
        mid_size = (ask_sizes+bid_sizes)/2
        mid_quotes = (ask_prices + bid_prices)/2
        
        quote_depth = mid_size*mid_quotes
        quote_depth.columns = ['level_{}_market_depth'.format(k) for k in range(1, (1+self.level_number))]

        return quote_depth
    
    
    def match_spot_quote(self, spot_data:pd.DataFrame, quote_data:pd.DataFrame):
        # match the time stamp for spot data and quotes
        # spot_data: processed, time, amount, price, side
        # quote_data: market order data, 10s
        if len(quote_data) == 0:
            merged_data = pd.DataFrame()
        else:
            quote_data = self.cal_quote_depth(quote_data)

            if 'time' in quote_data.columns:
                quote_data = quote_data.set_index('time')
            quote_data.index = pd.to_datetime(quote_data.index)
            if quote_data.index.tz is not None:
                quote_data.index = quote_data.index.tz_localize(None)
            
            merged_data = pd.merge_asof(spot_data[['amount', 'price']], quote_data, 
                                        left_index=True, right_index=True, direction='backward') # match timestamp (earlier and nearest)
            merged_data['spot_depth'] = merged_data['amount']*merged_data['price']

            merged_data = merged_data.drop(columns = ['price', 'amount'])
            merged_data = pd.concat([merged_data[['spot_depth']], merged_data.drop(columns = 'spot_depth')], axis = 1)
            
        return merged_data
                
            
    def save_data(self, data, output_file):
        # save data as csv file
        # output_file: should be ended with .csv
        output_file = Path(output_file)
        output_dir = output_file.parent
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        if self.freq == 'daily':
            data.to_csv(output_file) # daily data can be saved in one csv file
        elif self.freq == 'tick':
            rows_per_file = 50000
            num_files = len(data) // rows_per_file + (1 if len(data) % rows_per_file != 0 else 0)
            # save csv
            for i in range(num_files):
                start_row = i * rows_per_file
                end_row = start_row + rows_per_file
                data_subset = data.iloc[start_row:end_row, :] #0:49999, 50000:99999, ...
                # index of data should be datetime
                
                subset_file = output_dir / f"market_depth_{data_subset.index[0].strftime('%Y%m%d')}_{i+1}.csv"
                data_subset.to_csv(subset_file)
 

    def integrate_singlemonth_depth(self, month_spot, json_file):
        date_quote_dict = self.load_from_json(json_file)
        depth_dfs = []
        if date_quote_dict is None:
            return None
        for date in month_spot['date'].unique():
            date_spot = month_spot[month_spot['date'] == date]
            date_quote = date_quote_dict[date]
            date_merge = self.match_spot_quote(date_spot, date_quote) 
            depth_dfs.append(date_merge)
        
        if self.freq == 'daily':
            depth_df = pd.concat(depth_dfs, axis = 0).resample('D').mean()
        else:
            depth_df = pd.concat(depth_df, axis=0)
            
        return depth_df
    
    def run(self, csv_file, json_name_template, output_file='../result/depth.csv'):
        
        # spot data
        market_spot = pd.read_csv(csv_file)
        market_spot = process_spot_data(market_spot, self.start_date)
        market_spot['month'] =  market_spot.index.strftime('%Y_%m')
        market_spot['month'] = market_spot['month'].apply(month_format)
        market_spot['date'] =  market_spot.index.strftime('%Y-%m-%d')
        month_list = market_spot['month'].unique()
        
        depth_dfs = []
        for month in month_list:
            month_spot = market_spot[market_spot['month'] ==month]
            json_file = json_name_template.format(month=month)
            data = self.integrate_singlemonth_depth(month_spot, json_file)
            if data is None:
                continue
            depth_dfs.extend(data)
            print('market depth for '+month+' has been calculated')
        print("all done.")
        market_depth = pd.concat(depth_dfs, axis = 0)

        # save result
        self.save_data(market_depth, output_file)
        
        return market_depth
    
    def plot_depth(self, market_depth:pd.DataFrame, market_name: str, level, output_dir='../figures/depth/'):
        # level: 1-100
        # level: spot level = 0
        market_depth = market_depth[market_depth.index>=pd.to_datetime(self.start_date)]
        dates = market_depth.index
        depth_level = market_depth.columns[level-1]

        depths = market_depth[depth_level].values
        plot_info = {
            "1":{
            "X":dates,
            "Y":depths,
            "type":"line",
            "label": depth_level,
            "ylabel": depth_level,
            "legend":[depth_level],
            "xticks":dates[::5],
            "xticklabels":[
                dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
            ],
            "axvline":pd.to_datetime(self.mark_date)}
        }
        title = depth_level + ' for ' + market_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plot_axis(plot_info, title, output_dir, file_type='png', fontsize=20)
            
    
    def plot_all_depth(self, market_depth:pd.DataFrame, market_name: str, levels, output_dir='../figures/depth/'):
        # levels: first 'levels' levels
        market_depth = market_depth[market_depth.index>=pd.to_datetime(self.start_date)]
        dates = market_depth.index
        depths = market_depth.iloc[:,:levels]
        plot_info = {
            "1":{
            "X": dates,
            "Y": depths.values,
            "type": "line",
            "label": list(depths.columns)[:levels],
            "ylabel": 'market depth',
            "legend": list(depths.columns)[:levels],
            "xticks": dates[::5],
            "xticklabels":[
                    dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
                ],
                "axvline":pd.to_datetime(self.mark_date)
            }
        }
        title = 'first_'+str(levels) +'market depth for ' +market_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plot_axis(plot_info, title, output_dir, file_type='png', fontsize = 20)
        
        
if __name__ == 'main':
    
    start_date = '20231101'
    mark_date = '20240111'
    level_number = 100
    market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']

    freq_daily = 'daily'
    for market_name in market_names:
        daily_depth = Depth(start_date, mark_date, level_number, freq_daily)
        market_depth = daily_depth.run('../data/spot/'+market_name+'-spot_spot.csv', 
                        '../data/market_order_json/'+market_name+'-spot_{month}.json',
                        output_file=f'../result/daily/{market_name}/market_depth.csv')
        for level in range(1,(level_number+1)):
            daily_depth.plot_spread(market_depth, market_name, level = level, output_dir=f'../figures/daily/market_depth/{market_name}/')
        for levels in iter([5, 10, 20, 50, 100]):
            daily_depth.plot_all_spread(market_depth, market_name, levels = levels, output_dir=f'../figures/daily_all/market_depth/{market_name}/')
        print('daily market depth for '+market_name+' has been saved')
        
        
    freq_tick = 'tick'
    for market_name in market_names:
        tick_depth = Depth(start_date, mark_date, level_number, freq_tick)
        market_depth = daily_depth.run('../data/spot/'+market_name+'-spot_spot.csv', 
                        '../data/market_order_json/'+market_name+'-spot_{month}.json',
                        output_file=f'../result/daily/{market_name}/market_depth.csv')
        print('tick market depth for '+market_name+' has been saved')