import json
import pandas as pd
import os

def json_to_csv():
    # result: same as csv: columns = time,market,level,coin_metrics_id,database_time,
    # ask_price,ask_size,bid_price,bid_size
    # convert to pd.DataFrame:
    quote_data_files = os.listdir('../data/market_order_json/')
    
    for quote_data_file in quote_data_files:
        if 'json' not in quote_data_file:
            continue
        
        with open('../data/market_order_json/'+quote_data_file) as f:
            quote_data = json.load(f)
    
        del quote_data['market'],quote_data['ask'],quote_data['bid']
        mid_quotes = []
        for date in quote_data:
            midquote_temp = [{'time': item['time'], 
                            'mid_quote': (float(item['asks'][0]['price']) + float(item['bids'][0]['price']))/2,
                            'ask_price': float(item['asks'][0]['price']),
                            'ask_size': float(item['asks'][0]['size']),         
                            'bid_price': float(item['bids'][0]['price']),
                            'bid_size': float(item['bids'][0]['size'])
                            }
                           for item in quote_data[date]]
            mid_quotes.extend(midquote_temp)

        mid_quotes = pd.DataFrame(mid_quotes)
        mid_quotes['date'] = mid_quotes['time'].apply(lambda x: x[:10])
        mid_quotes['time'] = mid_quotes['time'].apply(lambda x: x[11:19])

        mid_quotes['timestamp'] = pd.to_datetime(mid_quotes['date'].astype(str) + ' ' + mid_quotes['time'])
        mid_quotes = mid_quotes[['timestamp', 'ask_price', 'ask_size','bid_price', 'bid_size','mid_quote']]
        mid_quotes.columns = ['time', 'ask_price', 'ask_size','bid_price', 'bid_size','mid_quote']
        
        path = '../data/market_order_level1_processed/'
        if not os.path.exists(path):
            os.makedirs(path)
        save_name = quote_data_file.split('.')[0]
        mid_quotes.to_csv(path+save_name+'.csv')
        
    print('all level 1 data has been extracted and saved')


json_to_csv()
        
        
        