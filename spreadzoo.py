import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import glob
import json
import os
from pathlib import Path

# internal import
from utils import (process_spot_data, 
                   month_format, 
                   plot_axis)

''' effective spread'''
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


# calculate spread and plot
# data processing
class SpreadZoo:
    def __init__(self, start_date, mark_date, order_number:int):
        self.start_date = start_date
        self.mark_date = mark_date
        self.order_number = order_number

    def load_from_json(self, json_file):
        if os.path.exists(json_file):
            with open(json_file) as f:
                data_quote = json.load(f)
            return  json_to_df(data_quote)
        else:
            return None
    
    def price_weighted_quotes(self, market_name, quote_data, num_level):
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
    
    @staticmethod
    def match_spot_quote(spot_data:pd.DataFrame, quote_data:pd.DataFrame, order_level:str):
        # match the time stamp for spot data and quotes
        # spot_data: processed, time, amount, price, side
        # quote_data: market order data, 10s
        if len(quote_data) == 0:
            merged_data = pd.DataFrame()
        else:
            quote_data = quote_data[quote_data['level'] == order_level]
            # get mean of level 1 to level 5
            # quote_data = quote_data[quote_data['level'].isin(['level_1','level_2','level_3','level_4','level_5'])]
            # TODO: DEBUG merge code

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
    
    def plot_spread(self, espread:pd.DataFrame, market_name: str, output_dir='../figures/espread/'):
        espread = espread[espread.index>=pd.to_datetime(self.start_date)]
        dates = espread.index
        espreads = espread['espread'].values * 10000
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
            "axvline":pd.to_datetime(self.mark_date)}
        }
        title = 'espread(bps) for' + market_name
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plot_axis(plot_info, title, output_dir, file_type='png', fontsize=20)


class ESpread(SpreadZoo):
    def __init__(self, start_date, mark_date, order_number: int):
        super().__init__(start_date, mark_date, order_number)
    
    @staticmethod
    def cal_espread(merged_data:pd.DataFrame, daily = True, weight=False):
        # calculate effective spread
        if merged_data.empty:
            espread = pd.DataFrame()
        else:
            merged_data['espread'] = merged_data['side']*(merged_data['price'] - merged_data['mid_quote'])/merged_data['mid_quote']
            
            if daily == True:
                def amount_weighted_average(group):
                    weighted_spread = (group['espread'] * group['amount']).sum() / group['amount'].sum()
                    return pd.Series({'espread': weighted_spread})
                if weight:
                    espread = merged_data.resample('D').apply(amount_weighted_average)
                else:
                    espread = merged_data.resample('D').mean()
                espread = pd.DataFrame(espread)
            else:
                espread = merged_data[['espread']]

        return espread
    
    def integrate_singlemonth_spread(self, month_spot, json_file):
        date_quote_dict = self.load_from_json(json_file)
        espread_dfs = []
        if date_quote_dict is None:
            return None
        for date in month_spot['date'].unique():
            date_spot = month_spot[month_spot['date'] == date]
            date_quote = date_quote_dict[date]
            date_merge = self.match_spot_quote(date_spot, date_quote, str(self.order_number))
            date_espread = self.cal_espread(date_merge)  
            espread_dfs.append(date_espread)
        return espread_dfs
    
    def run(self, csv_file, json_name_template, output_file='../result/espread.csv'):
        # spot data
        market_spot = pd.read_csv(csv_file)
        market_spot = process_spot_data(market_spot)
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
        output_file = Path(output_file)
        output_dir =  output_file.parent
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        market_espread.to_csv(output_file)
        return market_espread


class RSpread(SpreadZoo):
    def __init__(self, start_date, mark_date, order_number: int):
        super().__init__(start_date, mark_date, order_number)
    
    def run(self, csv_file, output_file='../result/rspread.csv'):
        # spot data
        pass









