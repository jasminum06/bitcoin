import pandas as pd
import numpy as np
import os

def save_data(data, output_file):
    
    output_file = Path(output_file)
    output_dir = output_file.parent
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    data.to_csv(output_file)
    
def cal_VR(merged_data:pd.DataFrame, q: int, data_type = 'return', freq = 'daily'):
    if data_type == 'return':
        # returns by tick
        log_return = np.log(merged_data[['price']]/merged_data[['price']].shift(1))
        log_return.columns = ['return']
        # compounded daily return
        if freq == 'daily':
            period_1_return = log_return.resample('D').comprod()
            period_q_return = period_1_return.rolling(window = q).comprod()
            VR = period_q_return.std()/period_1_return.std()
            return VR
        
        elif freq == 'tick':
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