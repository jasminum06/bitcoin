from variancezoo import VarianceZoo

start_date = '20231101'
mark_date = '20240111'
level_number = 100
market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']
data_types = ['return', 'price']
# daily
freq_daily = 'daily'
q = 7
for market_name in market_names:
    data_type = 'return'
    variance_std_daily = VarianceZoo(start_date, mark_date, level_number, 'std', q, freq_daily, data_type)
    variance_std = variance_std_daily.run('../data/spot/'+market_name+'-spot_spot.csv', 
                    '../data/market_order_json/'+market_name+'-spot_{month}.json',
                    output_file=f'../result/daily/{market_name}/'+data_type+'_variance_std.csv')  # TODO: output_file
    for level in range(level_number+1):
        variance_std_daily.plot_variance(variance_std, market_name, level = level, output_dir=f'../figures/daily/return_variance_std/{market_name}/')
    for levels in iter([5, 10, 20, 50, 100]):
        variance_std_daily.plot_all_variance(variance_std, market_name, levels = levels, output_dir=f'../figures/daily_all/return_variance_std/{market_name}/')
    print('daily variance_std for '+market_name+' has been saved')
    
    variance_ratio_daily = VarianceZoo(start_date, mark_date, level_number, 'ratio', q, freq_daily, data_type)
    variance_ratio = variance_ratio_daily.run('../data/spot/'+market_name+'-spot_spot.csv', 
                    '../data/market_order_json/'+market_name+'-spot_{month}.json',
                    output_file=f'../result/daily/{market_name}/'+data_type+'_variance_ratio.csv')
    for level in range(level_number+1):
        variance_ratio_daily.plot_variance(variance_ratio, market_name, level = level, output_dir=f'../figures/daily/return_variance_ratio/{market_name}/')
    for levels in iter([5, 10, 20, 50, 100]):
        variance_ratio_daily.plot_all_variance(variance_ratio, market_name, levels = levels, output_dir=f'../figures/daily/return_variance_ratio/{market_name}/')
    print('daily variance_ratio for '+market_name+' has been saved')
    
    data_type = 'price'
    variance_std_daily = VarianceZoo(start_date, mark_date, level_number, 'std', q, freq_daily, data_type)
    variance_std = variance_std_daily.run('../data/spot/'+market_name+'-spot_spot.csv', 
                    '../data/market_order_json/'+market_name+'-spot_{month}.json',
                    output_file=f'../result/daily/{market_name}/'+data_type+'_variance_std.csv')  # TODO: output_file
    for level in range(level_number+1):
        variance_std_daily.plot_variance(variance_std, market_name, level = level, output_dir=f'../figures/daily/price_variance_std/{market_name}/')
    for levels in iter([5, 10, 20, 50, 100]):
        variance_std_daily.plot_all_variance(variance_std, market_name, levels = levels, output_dir=f'../figures/daily_all/price_variance_std/{market_name}/')
    print('daily variance_std for '+market_name+' has been saved')
    
    
        
freq_tick = 'tick'
q_list = [5, 15, 30]
for q in q_list:
    for market_name in market_names:
        for data_type in data_types:
            variance_std_tick = VarianceZoo(start_date, mark_date, level_number, 'std', q, freq_tick, data_type)
            variance_std = variance_std_tick.run('../data/spot/'+market_name+'-spot_spot.csv', 
                            '../data/market_order_json/'+market_name+'-spot_{month}.json',
                            output_file=f'../result/tick/{market_name}/'+data_type+'_variance_std.csv')
            if data_type == 'price':
                for level in range(level_number+1):
                    variance_std_tick.plot_variance(variance_std, market_name, level = level, output_dir=f'../figures/tick/{data_type}_variance_std/{market_name}/')
                for levels in iter([5, 10, 20, 50, 100]):
                    variance_std_daily.plot_all_variance(variance_std, market_name, levels = levels, output_dir=f'../figures/tick_all/{data_type}_variance_std/{market_name}/')
                print('tick price variance_std for '+market_name+' has been saved')
                
            variance_ratio_tick = VarianceZoo(start_date, mark_date, level_number, 'ratio', q, freq_tick, data_type)
            variance_ratio = variance_std_tick.run('../data/spot/'+market_name+'-spot_spot.csv', 
                            '../data/market_order_json/'+market_name+'-spot_{month}.json',
                            output_file=f'../result/tick/{market_name}/'+data_type+'_variance_ratio.csv')
            if data_type == 'price':
                for level in range(level_number+1):
                    variance_ratio_tick.plot_variance(variance_ratio, market_name, level = level, output_dir=f'../figures/tick/{data_type}_variance_ratio/{market_name}/')
                for levels in iter([5, 10, 20, 50, 100]):
                    variance_ratio_daily.plot_all_variance(variance_ratio, market_name, levels = levels, output_dir=f'../figures/tick_all/{data_type}_variance_ratio/{market_name}/')
                print('tick price variance_ratio for '+market_name+' has been saved')
            
