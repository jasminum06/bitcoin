import pandas as pd
import numpy as np
import os
from pathlib import Path

def save_data(data, output_file):
    
    output_file = Path(output_file)
    output_dir = output_file.parent
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    data.to_csv(output_file)
    
def cal_VR(merged_data:pd.DataFrame, q: int, delta_t = 5, data_type = 'return', freq = 'daily'):
    
    def rolling_std_cum(df):
        rolling_std = []
        for i in range(len(df)):
            window = df.iloc[0:i+1, :]
            std = window.std()
            rolling_std.append(std)
        return pd.Series(rolling_std, name='rolling_std')
    
    if data_type == 'return':
        # returns by tick
        log_return = np.log(merged_data[['price']]/merged_data[['price']].shift(1))
        log_return.columns = ['return']
        # compounded daily return
        if freq == 'daily':
            period_1_return = log_return.resample('D').comprod()
            period_q_return = period_1_return.rolling(window = q).apply(rolling_std_cum)
            VR = period_q_return/period_1_return.std()
            return VR
        
        elif freq == 'tick':
            period_q_return = log_return.groupby(pd.Grouper(freq=str(delta_t)+'min')).comprod().std()
            period_1_return = 
            period_1_return = log_return
            period_q_return = period_q_return
            
    elif data_type == 'price':
        # prices by tick
        if freq == 'daily':
            period_1_variance = merged_data[['price']].resample('D').std()
            period_q_variance = merged_data[['price']].resample('D').rolling(window = q).std()
            VR = period_q_variance/period_1_variance
        elif freq == 'tick':
            pass