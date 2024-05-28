import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt

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
    
market_names = ['bitstamp-btc-usd','gemini-btc-usd','coinbase-btc-usd','coinbase-eth-usd']
for market_name in market_names:
    data = pd.read_csv('../data/spot/'+market_name+'-spot_spot.csv')
    data = data.set_index('time')
    data.index = pd.to_datetime(data.index)
    data = data[data.index>=pd.to_datetime('20231101')]
    volatility = data[['price']].resample('D').std()
    volatility.columns = ['volatility']
    dates = volatility.index
    vol = volatility['volatility'].values
    plot_info = {
        "1":{
        "X":dates,
        "Y":vol,
        "type":"line",
        "label":"volatility",
        "ylabel":"volatility",
        "legend":['volatility'],
        "xticks":dates[::5],
        "xticklabels":[
            dt.datetime.strftime(date, '%Y-%m-%d') for date in dates[::5]
        ],
        "axvline":pd.to_datetime('20240111')}
    }
    title = 'daily volatility for' + market_name
    outputdir = "../figures/"
    
    plot_axis(plot_info, title, outputdir, file_type='png', fontsize=20)
    print(market_name +' done')
    