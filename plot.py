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

def plot_figures(mark_date:str, start_date:str, market_name:str):
    
    data= pd.read_csv('../result/'+market_name+'/adverse_selection.csv').set_index('time')
    data.index = pd.to_datetime(data.index)
    if data.index.tz is not None:
        data.index = data.index.tz_localize(None)
    data =data[data.index>=pd.to_datetime(start_date)]
    dates = data.index
    print(data.columns)
    data = data['adv_selection'].values
    plot_info = {
        "1":{
        "X":dates,
        "Y":data,
        "type":"line",
        "label":"adverse_selection",
        "ylabel":"adverse_selection",
        "legend":['adverse_selection'],
        "xticks":dates[::5],
        "xticklabels":[
            dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
        ],
        "axvline":pd.to_datetime(mark_date)}
    }
    title = 'adverse_selection for' + market_name
    outputdir = "../figures/adverse_selection/"
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    
    plot_axis(plot_info, title, outputdir, file_type='png', fontsize=20)

start_date = '20231101'
mark_date = '20240111'
market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']
for market_name in market_names:
    plot_figures(mark_date, start_date, market_name)
    print('adverse_selection for '+market_name+' has been plotted')
    
    
    