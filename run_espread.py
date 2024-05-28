from spreadzoo import ESpread # RSpread

start_date = '20231101'
mark_date = '20240111'
mySpreadZoo = ESpread(start_date, mark_date, order_number=1)

market_names = ['bitstamp-btc-usd','coinbase-btc-usd','coinbase-eth-usd','gemini-btc-usd']
for market_name in market_names:
    # mySpreadZoo.price_weighted_quotes(market_name, num_level=5)

    espread = mySpreadZoo.run('../data/spot/'+market_name+'-spot_spot.csv', 
                                      '../data/market_order_json/'+market_name+'-spot_{month}.json',
                                  output_file=f'../result/{market_name}/espread.csv'
                )
    mySpreadZoo.plot_spread(espread, market_name, output_dir=f'../figures/espread/{market_name}/')
    print('result for '+market_name+' has been saved')