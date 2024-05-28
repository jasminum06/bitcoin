import pandas as pd
import numpy as np
from scipy.stats import skew, kurtosis, jarque_bera
import matplotlib.pyplot as plt
import os

market_names = ['bitstamp-btc-usd', 'gemini-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd']
for market_name in market_names:
    file_path = '../result/'+market_name+'/VPIN.csv'
    data = pd.read_csv(file_path)
    data = data[['VPIN_real', 'VPIN_LR', 'VPIN_BVC']]
    for column in data.columns:
        vpin_data = data[column]
        mean = vpin_data.mean()
        std = vpin_data.std()
        maxv = vpin_data.max()
        minv = vpin_data.min()
        skewness = skew(vpin_data)
        excess_kurtosis = kurtosis(vpin_data)
        jb_stat, jb_p_value = jarque_bera(vpin_data)
        count = len(vpin_data)
        corr = vpin_data.corr(data['VPIN_real'])
        
        
        print('-------'+market_name+'-------')
        print("Mean:", mean)
        print("Standard Deviation:", std)
        print("Skewness:", skewness)
        print("max:",maxv)
        print("min:",minv)
        print("Excess Kurtosis:", excess_kurtosis)
        print("Jarque-Bera Statistic:", jb_stat)
        print("Total number:",count)
        print("corr with real:",corr)


    # plot
    plt.figure(figsize=(16,12))
    plt.plot(data)
    plt.legend(data.columns, loc = 'upper right')
    plt.tight_layout(pad=2.0)
    plt.title('VPIN for '+market_name, fontsize=20)
    if not os.path.exists('../figures/VPIN/'):
        os.makedirs('../figures/VPIN/')
    plt.savefig(
        '../figures/VPIN/' + 'VPIN for '+market_name + ".png",
    )
    plt.close()
    
    
            
