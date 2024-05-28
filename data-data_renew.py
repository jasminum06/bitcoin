'''
This file is for renewing the overall spot data.
'''
import csv
import glob

market_names = ['bitstamp-btc-usd', 'gemini-btc-usd', 'coinbase-btc-usd', 'coinbase-eth-usd']
for market_name in market_names:
    path_all = '../data/spot/'
    path_market = path_all+market_name+'-spot/'
    files = glob.glob(path_market+'spot_202405*.csv')
    save_path = path_all+market_name+'-spot_spot.csv'
    
    with open(save_path, 'a', newline = '') as output_file:
        writer = csv.writer(output_file)
        
        for file in files:
            with open(file, 'r', newline='') as input_file:
                reader = csv.reader(input_file)
                next(reader)
                for data in reader:
                   writer.writerow(data)
                   
    print('data for '+market_name+' has been renewed.')
            
        
    