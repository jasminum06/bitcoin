from spreadzoo import ESpread, RSpread, Adverse_Selection, BASpread

start_date = '20231101'
mark_date = '20240111'
espread_obj = ESpread(start_date, mark_date, order_number=5)

market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']
for market_name in market_names:
    # mySpreadZoo.price_weighted_quotes(market_name, num_level=5)

    
    #TODO: merged data保存问题：没办法保存整体
    espread = espread_obj.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                      '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                  output_file=f'../result/{market_name}/espread.csv'
                )
    espread_obj.plot_spread(espread, market_name, output_dir=f'../figures/espread/{market_name}/')
    print('result for '+market_name+' has been saved')