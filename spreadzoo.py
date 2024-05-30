import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import os
from pathlib import Path
import datetime as dt

# TODO： code问题：save中间变量；有一些步骤有重复

# internal import
from utils import (process_spot_data, 
                   month_format, 
                   plot_axis)

''' effective spread'''
def save_data(data, output_file):
    
    output_dir = output_file.parent
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    data.to_csv(output_file)
    
    
# calculate spread and plot
# data processing
class SpreadZoo:
    def __init__(self, start_date, mark_date, level_number:int, freq:str, spread_name:str):
        self.start_date = start_date
        self.mark_date = mark_date
        self.level_number = level_number # level_number >=20
        self.freq = freq # freq = 'tick' or 'daily'
        self.spread_name = spread_name
        
        
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
    
    def price_mid_quotes(self, quote_data):
        # level_list = ['level_{}'.format(i) for i in range(self.level_number)]
        # filtered_quotes = quote_data[quote_data['level'].isin(level_list)]
        
        quote_data = quote_data.sort_values(by =['time','level'])
        ask_prices = quote_data.pivot(index = 'time', columns = 'level', values = 'ask_price')
        bid_prices = quote_data.pivot(index = 'time', columns = 'level', values = 'bid_price')
        
        def weighted_avg(row):
            values = row.values
            total = values.sum()
            weights = values / total
            weighted_sum = (values * weights).sum() # TODO: 根据price加权还是根据amount加权？
            return weighted_sum
        
        for i in iter([5,20]):    
            ask_prices['weighted_ask_{i}'] = ask_prices.iloc[:,:i].apply(weighted_avg, axis = 1)
            bid_prices['weighted_bid_{i}'] = bid_prices.iloc[:,:i].apply(weighted_avg, axis = 1)
        
        mid_quotes = (ask_prices + bid_prices)/2
        mid_quotes.columns = ['level_{}_mid_quote' for k in range(1, (1+self.level_number))] + \
                             ['weighted_5_levels_mid_quote', 'weighted_20_levels_mid_quote']
        
        return mid_quotes
    
    
    def match_spot_quote(self, spot_data:pd.DataFrame, quote_data:pd.DataFrame):
        # match the time stamp for spot data and quotes
        # spot_data: processed, time, amount, price, side
        # quote_data: market order data, 10s
        if len(quote_data) == 0:
            merged_data = pd.DataFrame()
        else:
            quote_data = self.price_mid_quotes(quote_data)

            if 'time' in quote_data.columns:
                quote_data = quote_data.set_index('time')
            quote_data.index = pd.to_datetime(quote_data.index)
            if quote_data.index.tz is not None:
                quote_data.index = quote_data.index.tz_localize(None)
            
            merged_data = pd.merge_asof(spot_data[['amount', 'price', 'side']], quote_data, 
                                        left_index=True, right_index=True, direction='backward') # match timestamp (earlier and nearest)
            merged_data['side'] = merged_data['side'].replace(0,-1) # buy side ==1, sell side == -1
        
        return merged_data
    
    def plot_spread(self, spread:pd.DataFrame, market_name: str, level, output_dir='../figures/spread/'):
        # level <= self.level_number +2
        # level+1: weighted 5 levels
        # level+2: weighted 20 levels
        spread = spread[spread.index>=pd.to_datetime(self.start_date)]
        dates = spread.index
        spread_level = spread.columns[level-1]
        spreads = spread[spread_level].values * 10000
        plot_info = {
            "1":{
            "X":dates,
            "Y":spreads,
            "type":"line",
            "label": spread_level+ ' '+ self.spread_name+ "(bps)",
            "ylabel": spread_level+' '+ self.spread_name+"spread(bps)",
            "legend":[spread_level+' '+ self.spread_name+'(bps)'],
            "xticks":dates[::5],
            "xticklabels":[
                dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
            ],
            "axvline":pd.to_datetime(self.mark_date)}
        }
        title = spread_level +' '+ self.spread_name +'(bps) for' + market_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plot_axis(plot_info, title, output_dir, file_type='png', fontsize=20)



class ESpread(SpreadZoo):
    def __init__(self, start_date, mark_date, order_number: int, freq:str, spread_name):
        super().__init__(start_date, mark_date, order_number, freq, spread_name)
    
    
    def cal_espread(self, merged_data:pd.DataFrame, weight=False):
        # calculate effective spread
        if merged_data.empty:
            espread = pd.DataFrame()
        else:
            merged_data_mid_quotes = merged_data.drop(columns = ['side', 'amount', 'price'])
            espread = merged_data['side'].values[:, None]*(-merged_data_mid_quotes.subtract(merged_data['price']))/merged_data_mid_quotes
            espread = pd.concat([espread, merged_data[['amount']]], axis =1)
            
            if self.freq == 'daily':
                def amount_weighted_average(group):
                    weighted_spread = (group.drop(columns = ['amount']) * group['amount'].values[:, None]).sum(axis=0) / group['amount'].sum()
                    return weighted_spread
                if weight:
                    espread = espread.resample('D').apply(amount_weighted_average)
                else:
                    espread = espread.drop(columns = ['amount']).resample('D').mean()
                    
            elif self.freq == 'tick':
                espread = espread.drop(columns = ['amount'])

        return espread
    
    def integrate_singlemonth_spread(self, month_spot, json_file):
        date_quote_dict = self.load_from_json(json_file)
        espread_dfs = []
        if date_quote_dict is None:
            return None
        for date in month_spot['date'].unique():
            date_spot = month_spot[month_spot['date'] == date]
            date_quote = date_quote_dict[date]
            date_merge = self.match_spot_quote(date_spot, date_quote)
            date_espread = self.cal_espread(date_merge)  
            espread_dfs.append(date_espread)
        return espread_dfs
    
    def run(self, csv_file, json_name_template, output_file='../result/espread.csv',merged_data= None):
        
        if merged_data is not None:
            market_espread = self.cal_espread(merged_data)
        else:
            # spot data
            market_spot = pd.read_csv(csv_file)
            market_spot = process_spot_data(market_spot, self.start_date)
            market_spot['month'] =  market_spot.index.strftime('%Y_%m')
            market_spot['month'] = market_spot['month'].apply(month_format)
            market_spot['date'] =  market_spot.index.strftime('%Y-%m-%d')
            month_list = market_spot['month'].unique()
            
            espread_dfs = []
            for month in month_list:
                month_spot = market_spot[market_spot['month'] ==month]
                json_file = json_name_template.format(month=month)
                data = self.integrate_singlemonth_spread(month_spot, json_file)
                if data is None:
                    continue
                espread_dfs.extend(data)
                print('espread for '+month+' has been calculated')
            print("all done.")
            market_espread = pd.concat(espread_dfs, axis = 0)

        # save result
        self.save(market_espread, output_file)
        
        return market_espread


class RSpread(SpreadZoo):
    def __init__(self, start_date, mark_date, order_number: int, freq: str, spread_name, dt):
        super().__init__(start_date, mark_date, order_number, freq, spread_name)
        self.dt = dt
    
    
    def match_spot_quote(self, spot_data:pd.DataFrame, quote_data:pd.DataFrame):
        # match the time stamp for spot data and quotes
        # spot_data: processed, time, amount, price, side
        # quote_data: market order data, 10s
        if len(quote_data) == 0:
            merged_data = pd.DataFrame()
        else:
            quote_data = self.price_weighted_quotes(quote_data)

            if 'time' in quote_data.columns:
                quote_data = quote_data.set_index('time')
            quote_data.index = pd.to_datetime(quote_data.index)
            if quote_data.index.tz is not None:
                quote_data.index = quote_data.index.tz_localize(None)
            
            merged_data = pd.merge_asof(spot_data[['amount', 'price', 'side']], quote_data[['mid_quote']], 
                                        left_index=True, right_index=True, direction='backward') # match timestamp (earlier and nearest)
            

            quote_data_lag = quote_data[['mid_quote']]
            quote_data_lag.index = quote_data_lag.index - pd.Timedelta(minutes=self.dt)   # TODO: 月末和月初的衔接???
            quote_data_lag.columns = ['mid_quote_lag']
            merged_data = pd.merge_asof(merged_data[['amount', 'price', 'side','mid_quote']], quote_data_lag,
                               left_index=True, right_index=True, direction='forward')
            
            merged_data['side'] = merged_data['side'].replace(0,-1) # buy side ==1, sell side == -1
        
        return merged_data
    
    def cal_rspread(self, merged_data, weight = False):
        
        if merged_data.empty:
            return pd.DataFrame()
        else:
            merged_data['rspread'] = merged_data['side']*(merged_data['price']-merged_data['mid_quote_lag'])/merged_data['mid_quote']
        if self.freq == 'daily':
            def amount_weighted_average(group):
                weighted_spread = (group['rspread'] * group['amount']).sum() / group['amount'].sum()
                return pd.Series({'rspread': weighted_spread})
            if weight:
                rspread = merged_data.resample('D').apply(amount_weighted_average)
            else:
                rspread = merged_data['rspread'].resample('D').mean()
            rspread = pd.DataFrame(rspread)
        elif self.freq == 'tick':
            rspread = merged_data[['rspread']]

        return rspread

    def integrate_singlemonth_spread(self, month_spot, json_file):
        date_quote_dict = self.load_from_json(json_file)
        rspread_dfs = []
        if date_quote_dict is None:
            return None
        for date in month_spot['date'].unique():
            date_spot = month_spot[month_spot['date'] == date]
            date_quote = date_quote_dict[date]
            date_merge = self.match_spot_quote(date_spot, date_quote)
            date_rspread = self.cal_rspread(date_merge)  
            rspread_dfs.append(date_rspread)
        return rspread_dfs
    
    def run(self, csv_file, json_name_template, output_file='../result/rspread.csv', merged_data = None):
        # spot data
        if merged_data is not None:
            market_rspread = self.cal_rspread(merged_data)
        else: 
            # spot data
            market_spot = pd.read_csv(csv_file)
            market_spot = process_spot_data(market_spot, self.start_date)
            market_spot['month'] =  market_spot.index.strftime('%Y_%m')
            market_spot['month'] = market_spot['month'].apply(month_format)
            market_spot['date'] =  market_spot.index.strftime('%Y-%m-%d')
            month_list = market_spot['month'].unique()
            
            rspread_dfs = []
            for month in month_list:
                month_spot = market_spot[market_spot['month'] ==month]
                json_file = json_name_template.format(month=month)
                data = self.integrate_singlemonth_spread(month_spot, json_file)
                if data is None:
                    continue
                rspread_dfs.extend(data)
                print('rspread for '+month+' has been calculated')
            print("all done.")
            market_rspread = pd.concat(rspread_dfs, axis = 0)

        # save result
        self.save(market_rspread, output_file)
        
        return market_rspread



class Adverse_Selection(RSpread):
    def __init__(self, start_date, mark_date, order_number: int, freq:str, spread_name, dt):
        super().__init__(start_date, mark_date, order_number, freq, dt, spread_name)

    def cal_adv_selection(self, merged_data, weight = False):
        if merged_data.empty:
            return pd.DataFrame()
        else:
            merged_data['adverse_selection'] = merged_data['side']*(merged_data['mid_quote_lag'] - merged_data['mid_quote'])/merged_data['mid_quote']
        
        if self.freq == 'daily':
            def amount_weighted_average(group):
                weighted_spread = (group['adverse_selection'] * group['amount']).sum() / group['amount'].sum()
                return pd.Series({'adverse_selection': weighted_spread})
            if weight:
                adv_selection = merged_data.resample('D').apply(amount_weighted_average)
            else:
                adv_selection = merged_data['adverse_selection'].resample('D').mean()
            adv_selection = pd.DataFrame(adv_selection)
        else:
            adv_selection = merged_data[['adverse_selection']]

        return adv_selection  
        
    def integrate_singlemonth_spread(self, month_spot, json_file):
        date_quote_dict = self.load_from_json(json_file)
        adv_selection_dfs = []
        if date_quote_dict is None:
            return None
        for date in month_spot['date'].unique():
            date_spot = month_spot[month_spot['date'] == date]
            date_quote = date_quote_dict[date]
            date_merge = self.match_spot_quote(date_spot, date_quote)
            date_adv_selection = self.cal_adv_selection(date_merge)  
            adv_selection_dfs.append(date_adv_selection)
        return adv_selection_dfs
    
    def run(self, csv_file, json_name_template, output_file='../result/adverse_selection.csv', merged_data = None):
        # spot data
        if merged_data is not None:
            market_adv_selection = self.cal_adv_selection(merged_data)
        else: 
            # spot data
            market_spot = pd.read_csv(csv_file)
            market_spot = process_spot_data(market_spot, self.start_date)
            market_spot['month'] =  market_spot.index.strftime('%Y_%m')
            market_spot['month'] = market_spot['month'].apply(month_format)
            market_spot['date'] =  market_spot.index.strftime('%Y-%m-%d')
            month_list = market_spot['month'].unique()
            
            adverse_selection_dfs = []
            for month in month_list:
                month_spot = market_spot[market_spot['month'] ==month]
                json_file = json_name_template.format(month=month)
                data = self.integrate_singlemonth_spread(month_spot, json_file)
                if data is None:
                    continue
                adverse_selection_dfs.extend(data)
                print('adverse selection for '+month+' has been calculated')
            print("all done.")
            market_adv_selection = pd.concat(adverse_selection_dfs, axis = 0)

        # save result
        self.save(market_adv_selection, output_file)
        
        return market_adv_selection
    

class BASpread(SpreadZoo):
    def __init__(self, start_date, mark_date, order_number: int, freq:str, spread_name):
        super().__init__(start_date, mark_date, order_number, freq, spread_name)

    
    def cal_baspread(self, merged_data):
        
        merged_data['bid_ask_spread'] = merged_data['bid_price'] - merged_data['ask_price']
        if self.freq == 'daily':
            return merged_data[['bid_ask_spread']].resample('D').mean()
        else:
            return merged_data[['bid_ask_spread']]
    
    def integrate_singlemonth(self, month_spot, json_file):
        date_quote_dict = self.load_from_json(json_file)
        baspread_dfs = []
        if date_quote_dict is None:
            return None
        for date in month_spot['date'].unique():
            date_spot = month_spot[month_spot['date'] == date]
            date_quote = date_quote_dict[date]
            date_merge = self.match_spot_quote(date_spot, date_quote)
            date_baspread = self.cal_adv_selection(date_merge)  
            baspread_dfs.append(date_baspread)
        return baspread_dfs
    
    def run(self, csv_file, json_name_template, output_file='../result/rspread.csv', merged_data = None):
        if merged_data is not None:
            return self.cal_baspread(merged_data)  # TODO: 没办法更改daily的bool值 # 可以在init里加
        else:
            market_spot = pd.read_csv(csv_file)
            market_spot = process_spot_data(market_spot, self.start_date)
            market_spot['month'] =  market_spot.index.strftime('%Y_%m')
            market_spot['month'] = market_spot['month'].apply(month_format)
            market_spot['date'] =  market_spot.index.strftime('%Y-%m-%d')
            month_list = market_spot['month'].unique()
            
            baspread_dfs = []
            for month in month_list:
                month_spot = market_spot[market_spot['month'] ==month]
                json_file = json_name_template.format(month=month)
                data = self.integrate_singlemonth_spread(month_spot, json_file)
                if data is None:
                    continue
                baspread_dfs.extend(data)
                print('bid-ask spread for '+month+' has been calculated')
            print("all done.")
            market_baspread = pd.concat(baspread_dfs, axis = 0)

        # save result
        self.save(market_baspread, output_file)
        
        return market_baspread
        



