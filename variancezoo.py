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
    
class VarianceZoo():
    
    def __init__(self, start_date, mark_date, level_number, type:str, q, freq, data_type):
        self.start_date = start_date
        self.mark_date = mark_date
        self.level_number = level_number
        self.type = type # type = 'ratio' or 'std'
        self.q = q # q periods # if freq = 'daily', q days; if freq = 'tick', q mins
        self.freq = freq
        self.data_type = data_type # data_type = 'return' or 'price'
    
        
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
                data_subset.to_csv(output_file.split('.')[0]+'_'+data_subset.index[0].strftime('%Y%m%d')+f'_{i+1}.csv', index=False)
                

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
    
    
    def cal_VR(self, merged_data:pd.DataFrame):
        
        merged_data = merged_data.drop(columns = ['side', 'amount'])
        def rolling_std_cum(df):
            rolling_std = []
            for i in range(len(df)):
                window = df.iloc[0:i+1, :]
                std = window.std()
                rolling_std.append(std)
            return pd.DataFrame(rolling_std, index = df.index, columns = df.columns)
    
        if self.data_type == 'return':
            # returns by tick
            log_return = np.log(merged_data/merged_data.shift(1))
            log_return.columns = ['return']+ ['level_{}_midquote_return'.format(k) for k in range(1,1+self.level_number)] + \
                ['weighted_5_levels_midquote_return', 'weighted_20_levels_midquote_return']
            # compounded daily return
            if self.freq == 'daily':
                period_1_return = (log_return).resample('D').apply(np.sum)
                period_1_variance = rolling_std_cum(period_1_return)
                period_q_return = (period_1_return).rolling(window=self.q).apply(np.sum)
                period_q_variance = rolling_std_cum(period_q_return)
                VR = (period_q_variance/period_1_variance)
            
            elif self.freq == 'tick':
                period_1_return = (log_return).resample('1min').apply(np.sum) # 1min as benchmark
                period_1_variance = rolling_std_cum(period_1_return)
                period_q_return = period_1_return.rolling(window=self.q).apply(np.sum)
                period_q_variance = rolling_std_cum(period_q_return)
                VR = (period_q_variance/period_1_variance)
                
        elif self.data_type == 'price':
            # prices by daily
            if self.freq == 'daily':
                # period_1_variance = merged_data.resample('D').std()
                # period_q_variance = merged_data.resample('D').apply(rolling_std_cum)
                # VR = period_q_variance/period_1_variance
                pass
            elif self.freq == 'tick': # qmin/1min
                period_1_variance = merged_data.resample('1min').head(1).resample('D').std()
                period_q_variance = merged_data.resample(str(self.q) + 'min').head(1).resample('D').std()
                VR = period_q_variance/period_1_variance
        VR.columns = ['level_{}'.format(k) for k in range(1+self.level_number)] + \
                ['weighted_5_levels', 'weighted_20_levels']
        return VR
    
    def cal_std(self, merged_data):
        
        merged_data = merged_data.drop(columns = ['side', 'amount'])
        if self.data_type == 'return':
            log_return = np.log(merged_data/merged_data.shift(1))
            log_return.columns = ['return']+ ['level_{}_midquote_return'.format(k) for k in range(1,1+self.level_number)] + \
                ['weighted_5_levels_midquote_return', 'weighted_20_levels_midquote_return']
            if self.freq == 'daily':
                Var_std = log_return.resample('D').std() # all the data in each day
            elif self.freq == 'tick':
                Var_std = log_return.resample(str(self.q)+'min').apply(np.sum).resample('D').std() # larger range
        elif self.data_type == 'price':
            if self.freq == 'daily':
                Var_std = merged_data.resample('D').std() # all the data in each day
            elif self.freq == 'tick':
                Var_std = merged_data.resample(str(self.q)+'min').head().resample('D').std() # larger range
                
        Var_std.columns = ['level_{}'.format(k) for k in range(1+self.level_number)] + \
                ['weighted_5_levels', 'weighted_20_levels']
        return Var_std
                    
    def plot_variance(self, variance_data:pd.DataFrame, market_name: str, level = 0, output_dir='../figures/spread/'):
        # level: level = 0: spot
        # level = 1-20: market order
        # level = 21, 22: weighted price
        variance_data = variance_data[variance_data.index>=pd.to_datetime(self.start_date)]
        dates = variance_data.index
        variance_level = variance_data.columns[level]
            
        variances = variance_data[variance_level].values
        plot_info = {
            "1":{
            "X":dates,
            "Y":variances,
            "type":"line",
            "label": variance_level+ ' variance '+ self.type,
            "ylabel": variance_level+ ' variance '+ self.type,
            "legend":[variance_level+ ' variance '+ self.type],
            "xticks":dates[::5],
            "xticklabels":[
                dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
            ],
            "axvline":pd.to_datetime(self.mark_date)}
        }
        if self.type == 'std':
            if self.freq == 'daily':
                title = 'daily_'+variance_level +' '+self.data_type +' variance '+ self.type + ' for ' + market_name
            else:
                title = str(self.q)+'_min_'+variance_level +' '+self.data_type +' variance '+ self.type + ' for ' + market_name
        elif self.type == 'ratio':
            if self.freq == 'daily':
                title = str(self.q)+'_day_'+variance_level +' '+self.data_type +' variance '+ self.type + ' for ' + market_name
            else:
                title = str(self.q)+'_min_'+variance_level +' '+self.data_type +' variance '+ self.type + ' for ' + market_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plot_axis(plot_info, title, output_dir, file_type='png', fontsize=20)

    def plot_all_variance(self, variance_data:pd.DataFrame, market_name: str, levels, output_dir='../figures/spread/'):
        
        variance_data = variance_data[variance_data.index>=pd.to_datetime(self.start_date)]
        dates = variance_data.index
        variance_data = variance_data.iloc[:, :(levels+1)]
        
        plot_info = {
            "1":{
            "X":dates,
            "Y":variance_data.values,
            "type":"line",
            "label": list(variance_data.columns),
            "ylabel": 'variance '+ self.type,
            "legend":list(variance_data.columns),
            "xticks":dates[::5],
            "xticklabels":[
                dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
            ],
            "axvline":pd.to_datetime(self.mark_date)}
        }
        if self.type == 'std':
            title = 'first ' +str(levels)+' '+self.data_type +' variance '+ self.type + ' for ' + market_name
        elif self.type == 'ratio':
            if self.freq == 'daily':
                title = str(self.q)+'_day'+'first ' +str(levels)+' '+self.data_type +' variance '+ self.type + ' for ' + market_name
            else:
                title = str(self.q)+'_min'+'first ' +str(levels)+' '+self.data_type +' variance '+ self.type + ' for ' + market_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plot_axis(plot_info, title, output_dir, file_type='png', fontsize=20)
        
        
    def integrate_singlemonth_variance(self, month_spot, json_file):
        
        date_quote_dict = self.load_from_json(json_file)
        if date_quote_dict is None:
            return None
        dates_variance = []
        for date in month_spot['date'].unique():
            date_spot = month_spot[month_spot['date'] == date]
            date_quote = date_quote_dict[date]
            date_merge = self.match_spot_quote(date_spot, date_quote)
            if self.type == 'std':
                date_variance = date_merge.std()
            elif self.type == 'ratio':
                date_variance = self.cal_VR(date_merge)  
            dates_variance.append(date_variance)
        
        month_variance = pd.concat(dates_variance, axis=0)
        
        
        return month_variance
    
    
    def run(self, csv_file, json_name_template, output_file = '../result/variance.csv'):
        
        market_spot = pd.read_csv(csv_file)
        market_spot = process_spot_data(market_spot, self.start_date)
        market_spot['month'] =  market_spot.index.strftime('%Y_%m')
        market_spot['month'] = market_spot['month'].apply(month_format)
        market_spot['date'] =  market_spot.index.strftime('%Y-%m-%d')
        month_list = market_spot['month'].unique()
        
        variance_dfs = []
        for month in month_list:
            month_spot = market_spot[market_spot['month'] ==month]
            json_file = json_name_template.format(month=month)
            data = self.integrate_singlemonth_variance(month_spot, json_file)
            if data is None:
                continue
            variance_dfs.append(data)
            print('variance '+self.type +' for '+month+' has been calculated')
        print("all done.")
            
        variance_df = pd.concat(variance_dfs,axis = 0)
        
        # save data
        self.save_data(variance_df, output_file)
        
        return variance_df
    

