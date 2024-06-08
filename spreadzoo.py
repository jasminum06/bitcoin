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
            weighted_sum = (values * weights).sum() # TODO: 根据amount加权
            return weighted_sum
        
        for i in iter([5,20]):    
            ask_prices['weighted_{}_levels'.format(str(i))] = ask_prices.iloc[:,:i].apply(weighted_avg, axis = 1)
            bid_prices['weighted_{}_levels'.format(str(i))] = bid_prices.iloc[:,:i].apply(weighted_avg, axis = 1)
        
        mid_quotes = (ask_prices + bid_prices)/2
        mid_quotes.columns = ['level_{}_mid_quote'.format(k) for k in range(1, (1+self.level_number))] + \
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
        
        if self.spread_name == 'bid-ask spread':
            spreads = spreads[spread_level].values
            plot_info = {
                "1":{
                "X":dates,
                "Y":spreads,
                "type":"line",
                "label": spread_level+ ' '+ self.spread_name,
                "ylabel": spread_level+' '+ self.spread_name,
                "legend":[spread_level+' '+ self.spread_name],
                "xticks":dates[::5],
                "xticklabels":[
                    dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
                ],
                "axvline":pd.to_datetime(self.mark_date)}
            }
            title = spread_level +' '+ self.spread_name +' for' + market_name
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            plot_axis(plot_info, title, output_dir, file_type='png', fontsize=20)
            
        else:
            spreads = spread[spread_level].values * 10000
            plot_info = {
                "1":{
                "X":dates,
                "Y":spreads,
                "type":"line",
                "label": spread_level+ ' '+ self.spread_name+ "(bps)",
                "ylabel": spread_level+' '+ self.spread_name+"(bps)",
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
            
        
        plot_info = {
            "1":{
            "X": dates,
            "Y": spread.values*10000,
            "type": "line",
            "label": list(spread.columns)[:level],
            "ylabel": 'espread(bps)',
            "legend": list(spread.columns)[:level],
            "xticks": dates[::5],
            "xticklabels":[
                    dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
                ],
                "axvline":pd.to_datetime(self.mark_date)
            }
        }
        title = 'first_'+str(level) + '_levels_'+ self.spread_name+ '(bps) for ' +market_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plot_axis(plot_info, title, output_dir, file_type='png', fontsize = 20) 
            
            
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
            rows_per_file = 500000
            num_files = len(data) // rows_per_file + (1 if len(data) % rows_per_file != 0 else 0)
            # save csv
            for i in range(num_files):
                start_row = i * rows_per_file
                end_row = start_row + rows_per_file
                data_subset = data.iloc[start_row:end_row, :] #0:499999, 500000:9999999, ...
                # index of data should be datetime
                data_subset.to_csv(output_file.split('.')[0]+'_'+data_subset.index[0].strftime('%Y%m%d')+f'_{i+1}.csv', index=False)



class ESpread(SpreadZoo):
    def __init__(self, start_date, mark_date, order_number: int, freq:str, spread_name):
        super().__init__(start_date, mark_date, order_number, freq, spread_name)
    
    
    def cal_espread(self, merged_data:pd.DataFrame, weight=False):
        # calculate effective spread
        if merged_data.empty:
            espread = pd.DataFrame()
        else:
            merged_data_mid_quotes = merged_data.drop(columns = ['side', 'amount', 'price'])
            espread = merged_data['side'].values[:, None]*(-merged_data_mid_quotes.subtract(merged_data['price'], axis=0))/merged_data_mid_quotes
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

        espread.columns = ['level_{}_espread'.format(k) for k in range(1, (1+self.level_number))] + \
                             ['weighted_5_levels_espread', 'weighted_20_levels_espread']
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
        self.save_data(market_espread, output_file)
        
        return market_espread


class RSpread(SpreadZoo):
    def __init__(self, start_date, mark_date, order_number: int, freq: str, spread_name, delta_t):
        super().__init__(start_date, mark_date, order_number, freq, spread_name)
        self.delta_t = int(delta_t)
    
    
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
            

            quote_data_lag = quote_data
            quote_data_lag.index = quote_data_lag.index - pd.Timedelta(minutes=self.delta_t)   # TODO: 月末和月初的衔接???
            quote_data_lag.columns = ['level_{}_mid_quote_lag'.format(k) for k in range(1, (self.level_number+1))] +\
                                    ['weighted_5_levels_mid_quote_lag', 'weighted_20_levels_mid_quote_lag']
            merged_data = pd.merge_asof(merged_data, quote_data_lag,
                               left_index=True, right_index=True, direction='backward') # TODO: check: forward or backward?
            
            merged_data['side'] = merged_data['side'].replace(0,-1) # buy side ==1, sell side == -1
        
        return merged_data
    
    def cal_rspread(self, merged_data, weight = False):
        
        if merged_data.empty:
            return pd.DataFrame()
        else:
            merged_data_mid_quotes_lag = merged_data[['level_{}_mid_quote_lag'.format(k) for k in range(1, (self.level_number+1))] +['weighted_5_levels_mid_quote_lag', 'weighted_20_levels_mid_quote_lag']]
            merged_data_mid_quotes = merged_data[['level_{}_mid_quote'.format(k) for k in range(1, (self.level_number+1))] +['weighted_5_levels_mid_quote', 'weighted_20_levels_mid_quote']]
            merged_data_mid_quotes_lag.columns = merged_data_mid_quotes.columns
            rspread = merged_data['side'].values[:,None]*(-merged_data_mid_quotes_lag.subtract(merged_data['price'], axis = 0))/merged_data_mid_quotes
            rspread = pd.concat([rspread, merged_data[['amount']]], axis = 1)
            
        if self.freq == 'daily':
            def amount_weighted_average(group):
                    weighted_spread = (group.drop(columns = ['amount']) * group['amount'].values[:, None]).sum(axis=0) / group['amount'].sum()
                    return weighted_spread
            if weight:
                rspread = rspread.resample('D').apply(amount_weighted_average)
            else:
                rspread = rspread.drop(columns = ['amount']).resample('D').mean()
        elif self.freq == 'tick':
            rspread = rspread.drop(columns = ['amount'])
            
        rspread.columns = ['level_{}_rspread'.format(k) for k in range(1, (1+self.level_number))] + \
                             ['weighted_5_levels_rspread', 'weighted_20_levels_rspread']
            
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
        self.save_data(market_rspread, output_file)
        
        return market_rspread



class Adverse_Selection(RSpread):
    def __init__(self, start_date, mark_date, order_number: int, freq:str, spread_name, delta_t):
        super().__init__(start_date, mark_date, order_number, freq, spread_name,  delta_t)


    def cal_adv_selection(self, merged_data, weight = False):
        if merged_data.empty:
            return pd.DataFrame()
        else:
            merged_data_mid_quotes_lag = merged_data[['level_{}_mid_quote_lag'.format(k) for k in range(1, (self.level_number+1))] +['weighted_5_levels_mid_quote_lag', 'weighted_20_levels_mid_quote_lag']]
            merged_data_mid_quotes = merged_data[['level_{}_mid_quote'.format(k) for k in range(1, (self.level_number+1))] +['weighted_5_levels_mid_quote', 'weighted_20_levels_mid_quote']]
            merged_data_mid_quotes_lag.columns = merged_data_mid_quotes.columns
            
            adv_selection = merged_data['side'].values[:,None]*(merged_data_mid_quotes_lag- merged_data_mid_quotes)/merged_data_mid_quotes
            adv_selection = pd.concat([adv_selection, merged_data[['amount']]], axis =1)
            
        if self.freq == 'daily':
            def amount_weighted_average(group):
                    weighted_spread = (group.drop(columns = ['amount']) * group['amount'].values[:, None]).sum(axis=0) / group['amount'].sum()
                    return weighted_spread
            if weight:
                adv_selection = adv_selection.resample('D').apply(amount_weighted_average)
            else:
                adv_selection = adv_selection.drop(columns = ['amount']).resample('D').mean()
        else:
            adv_selection = adv_selection.drop(columns=['amount'])
        
        adv_selection.columns = ['level_{}_adv_selection'.format(k) for k in range(1, (1+self.level_number))] + \
                             ['weighted_5_levels_adv_selection', 'weighted_20_levels_adv_selection']

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
        self.save_data(market_adv_selection, output_file)
        
        return market_adv_selection
    

class BASpread(SpreadZoo):
    def __init__(self, start_date, mark_date, order_number: int, freq:str, spread_name):
        super().__init__(start_date, mark_date, order_number, freq, spread_name)

    def quote_baspread(self, quote_data):
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
            ask_prices['weighted_{}_levels'.format(str(i))] = ask_prices.iloc[:,:i].apply(weighted_avg, axis = 1)
            bid_prices['weighted_{}_levels'.format(str(i))] = bid_prices.iloc[:,:i].apply(weighted_avg, axis = 1)
        
        baspread = bid_prices - ask_prices
        baspread.columns = ['level_{}_baspread'.format(k) for k in range(1, (1+self.level_number))] + \
                             ['weighted_5_levels_baspread', 'weighted_20_levels_baspread']
        
        return baspread
    
    def match_spot_quote(self, spot_data:pd.DataFrame, quote_data:pd.DataFrame):
        # match the time stamp for spot data and quotes
        # spot_data: processed, time, amount, price, side
        # quote_data: market order data, 10s
        if len(quote_data) == 0:
            merged_data = pd.DataFrame()
        else:
            quote_data = self.quote_baspread(quote_data)

            if 'time' in quote_data.columns:
                quote_data = quote_data.set_index('time')
            quote_data.index = pd.to_datetime(quote_data.index)
            if quote_data.index.tz is not None:
                quote_data.index = quote_data.index.tz_localize(None)
            
            merged_data = pd.merge_asof(spot_data[['amount', 'price', 'side']], quote_data, 
                                        left_index=True, right_index=True, direction='backward') # match timestamp (earlier and nearest)
            merged_data['side'] = merged_data['side'].replace(0,-1) # buy side ==1, sell side == -1
        
        return merged_data
    
    def cal_baspread(self, merged_data):
        if merged_data.empty:
            baspread = pd.DataFrame()
        else:
            baspread = merged_data.drop(columns = ['side', 'amount', 'price'])
            
        if self.freq == 'daily':
            return baspread.resample('D').mean()
        else:
            return baspread
    
    def integrate_singlemonth_spread(self, month_spot, json_file):
        date_quote_dict = self.load_from_json(json_file)
        baspread_dfs = []
        if date_quote_dict is None:
            return None
        for date in month_spot['date'].unique():
            date_spot = month_spot[month_spot['date'] == date]
            date_quote = date_quote_dict[date]
            date_merge = self.match_spot_quote(date_spot, date_quote)
            date_baspread = self.cal_baspread(date_merge)  
            baspread_dfs.append(date_baspread)
        return baspread_dfs
    
    def run(self, csv_file, json_name_template, output_file='../result/baspread.csv', merged_data = None):
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
        self.save_data(market_baspread, output_file)
        
        return market_baspread
        



