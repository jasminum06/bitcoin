import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

''' spot data process'''
def process_spot_data(spot_data:pd.DataFrame, start_date = None):
    """Process data
    """
    if 'time' in spot_data.columns:
        spot_data = spot_data.set_index('time')
    spot_data.index = pd.to_datetime(spot_data.index)
    if spot_data.index.tz is not None:
        spot_data.index = spot_data.index.tz_localize(None)
        
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        spot_data = spot_data[spot_data.index>=start_date]
        
    return spot_data


def month_format(month_val: str):
    year, month = month_val.split('_')
    month = str(int(month))
    return year+'_'+month


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
            if len(X.shape) == 1:
                ax.plot(X, Y, color='royalblue', label=plot_info[key]["label"])
            elif len(X.shape) == 2:
                for col in range(X.shape[1]):
                    ax.plot(X[:, col], Y, label=plot_info[key]["label"][col], color=np.random.rand(3,))

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

