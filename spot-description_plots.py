import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import os

'''plot template'''
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


# fig2 - panel A: trade size
def plot_trade_size(market_name,start_date, amount_type = 'mean'):
    
    trade_size = pd.read_csv('../result/'+market_name+'/spot_'+ amount_type + '_trade_size.csv').set_index('time')
    trade_size.index = pd.to_datetime(trade_size.index)
    if start_date is not None:
        trade_size = trade_size[trade_size.index>=pd.to_datetime(start_date)]
    dates = trade_size.index
    amounts = trade_size['amount'].values
    plot_info = {
        "1":{
        "X":dates,
        "Y":amounts,
        "type":"line",
        "label":"amount",
        "ylabel":"amount_"+amount_type,
        "legend":['amount'],
        "xticks":dates[::5],
        "xticklabels":[
            date.to_pydatetime().strftime('%Y-%m-%d') for date in dates[::5]
        ],
        "axvline":pd.to_datetime(mark_date)}
    }
    title = 'trade size(' + amount_type + ') for' + market_name
    outputdir = "../figures/trade_size/"
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    
    plot_axis(plot_info, title, outputdir, file_type='png', fontsize=20)

# fig2 - panel B: number of trades
def plot_trade_counts(market_name,start_date):
    trade_counts = pd.read_csv('../result/'+market_name+'/spot_trade_counts.csv').set_index('time')
    trade_counts.index = pd.to_datetime(trade_counts.index)
    if start_date is not None:
        trade_counts = trade_counts[trade_counts.index>=pd.to_datetime(start_date)]
    dates = trade_counts.index
    counts = trade_counts['count'].values
    plot_info = {
        "1":{
        "X":dates,
        "Y":counts,
        "type":"line",
        "label":"number of trades",
        "ylabel":"number of trades",
        "legend":['number of trades'],
        "xticks":dates[::5],
        "xticklabels":[
            dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
        ],
        "axvline":pd.to_datetime(mark_date)}
    }
    title = 'daily number of trades for' + market_name
    outputdir = "../figures/trade_counts/"
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    
    plot_axis(plot_info, title, outputdir, file_type='png', fontsize=20)
    
    
# fig3 - panel A and B: percentage of trades
def plot_trade_percentage(market_name, start_date, threshold = 'mean'):
    
    trade_percentage = pd.read_csv('../result/'+market_name+'/spot_trade_percentage.csv').set_index('time')  
    trade_percentage.index = pd.to_datetime(trade_percentage.index)
    if start_date is not None:
        trade_percentage = trade_percentage[trade_percentage.index>=pd.to_datetime(start_date)]
        
    dates = trade_percentage.index
    large_percentages = trade_percentage['percentage_large'].values
    small_percentages = trade_percentage['percentage_small'].values
    
    plot_info = {
        "1":{
        "X":dates,
        "Y":large_percentages,
        "type":"line",
        "label":"percentage_large",
        "ylabel":"percentage of large trades",
        "legend":['percentage_large'],
        "xticks":dates[::5],
        "xticklabels":[
            dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
        ],
        "axvline":pd.to_datetime(mark_date)}
    }
    title = 'percentage of large trades for' + market_name
    outputdir = "../figures/trade_percentage/"
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    
    plot_axis(plot_info, title, outputdir, file_type='png', fontsize=20)
    
    plot_info = {
        "1":{
        "X":dates,
        "Y":small_percentages,
        "type":"line",
        "label":"percentage_small",
        "ylabel":"percentage of small trades",
        "legend":['percentage_small'],
        "xticks":dates[::5],
        "xticklabels":[
            dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
        ],
        "axvline":pd.to_datetime(mark_date)}
    }
    title = 'percentage of small trades for' + market_name
    
    plot_axis(plot_info, title, outputdir, file_type='png', fontsize=20)
    


start_date = '20231101'
mark_date = '20240111'
market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']
for market_name in market_names:
    plot_trade_size(market_name, start_date)
    plot_trade_counts(market_name, start_date)
    plot_trade_percentage(market_name, start_date)
    print('plots for '+market_name+' has been saved')








