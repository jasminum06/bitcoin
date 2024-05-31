from spreadzoo import ESpread, RSpread, Adverse_Selection, BASpread

start_date = '20231101'
mark_date = '20240111'
level_number = 20
market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']

# daily results:
freq_daily = 'daily'
for market_name in market_names:
    # 1
    espread_daily = ESpread(start_date, mark_date, level_number, freq = freq_daily, spread_name = 'espread')
    espread = espread_daily.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                output_file=f'../result/daily/{market_name}/effective_spread.csv'
                                )
    for level in range(1,23):
        espread_daily.plot_spread(espread, market_name, level = level, output_dir=f'../figures/daily/effective_spread/{market_name}/')    
    print('daily espread for '+market_name+' has been saved')
    
    # 2&3
    for delta_t in iter([5,15,30]):
        rspread_daily = RSpread(start_date, mark_date, level_number, freq = freq_daily, spread_name = 'rspread', delta_t = delta_t)
        adv_selection_daily = Adverse_Selection(start_date, mark_date, level_number, freq = freq_daily, spread_name = 'adverse_selection', delta_t= delta_t)
    
        rspread = rspread_daily.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                          '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                           output_file=f'../result/daily/{market_name}/realized_spread_'+str(delta_t)+'mins.csv'
                                         )
        adv_selection = adv_selection_daily.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                          '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                           output_file=f'../result/daily/{market_name}/adverse_selection_'+str(delta_t)+'mins.csv'
                                         )
        for level in range(1,23):
            rspread_daily.plot_spread(rspread, market_name, level=level, output_dir=f'../figures/daily/realized_spread/{market_name}/{str(delta_t)+'_mins'}/')
            adv_selection_daily.plot_spread(adv_selection, market_name, level=level, output_dir=f'../figures/daily/adverse_selection/{market_name}/{str(delta_t)+'_mins'}/')
        
        print('daily rspread and adverse selection for '+market_name+ 'with dt = '+str(delta_t)+'mins has been saved')
    
    # 4    
    baspread_daily = BASpread(start_date, mark_date, level_number, freq_daily, spread_name = 'bid-ask spread')
    baspread = baspread_daily.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                output_file=f'../result/daily/{market_name}/bid_ask_spread.csv'
                                )
    for level in range(1,23):
        baspread_daily.plot_spread(baspread, market_name, level = level, output_dir=f'../figures/daily/bid_ask_spread/{market_name}/')    
    print('daily baspread for '+market_name+' has been saved')



# TODO： 验证adverse_selction+ rspread    


# tick results
freq_tick = 'tick'
for market_name in market_names:
    # 1
    espread_tick = ESpread(start_date, mark_date, level_number, freq = freq_tick, spread_name = 'espread')
    espread = espread_tick.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                output_file=f'../result/tick/{market_name}/effective_spread.csv'
                                )
    for level in range(1,23):
        espread_tick.plot_spread(espread, market_name, level = level, output_dir=f'../figures/tick/effective_spread/{market_name}/')    
    print('tick espread for '+market_name+' has been saved')
    
    # 2&3
    for delta_t in iter([5,15,30]): #minitues match # TODO: forward or backward?
        rspread_tick = RSpread(start_date, mark_date, level_number, freq = freq_tick, spread_name = 'rspread', delta_t = delta_t)
        adv_selection_tick = Adverse_Selection(start_date, mark_date, level_number, freq = freq_tick, spread_name = 'adverse_selection', delta_t = delta_t)
        
        rspread = rspread_tick.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                          '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                           output_file=f'../result/tick/{market_name}/realized_spread_'+str(delta_t)+'mins.csv'
                                         )
        adv_selection = adv_selection_tick.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                          '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                           output_file=f'../result/tick/{market_name}/adverse_selection_'+str(delta_t)+'mins.csv'
                                         )
        for level in range(1,23):
            rspread_tick.plot_spread(rspread, market_name, level=level, output_dir=f'../figures/tick/realized_spread/{market_name}/{str(delta_t)+'_mins'}/')
            adv_selection_tick.plot_spread(adv_selection, market_name, level=level, output_dir=f'../figures/tick/adverse_selection/{market_name}/{str(delta_t)+'_mins'}/')
        
        print('tick rspread and adverse selection for '+market_name+ 'with dt = '+str(delta_t)+'mins has been saved')
    
    # 4    
    baspread_tick = BASpread(start_date, mark_date, level_number, freq= freq_tick, spread_name = 'bid-ask spread')
    baspread = baspread_tick.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                output_file=f'../result/tick/{market_name}/bid_ask_spread.csv'
                                )
    for level in range(1,23):
        baspread_tick.plot_spread(baspread, market_name, level = level, output_dir=f'../figures/tick/bid_ask_spread/{market_name}/')    
    print('tick baspread for '+market_name+' has been saved')