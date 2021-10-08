"""
    Title: Scanner for technical indicators
    Description: This strategy scans the NIFTY500 universe and prints 
        top and bottom n (=5) stocks sorted by their latest RSI number.
        This is done everyday, 30 mins after market open. Note: mind 
        how much is your print output, else you will get a overflow 
        error. Run it for a few days on daily dataset, else you may 
        encounter difficulties.
    Style tags: Technical indicator scanner
    Asset class: Equities, Futures, ETFs, Currencies
    Broker: NSE/ US Equities
"""
from blueshift_library.pipelines.pipelines import average_volume_filter, technical_factor
from blueshift_library.technicals.indicators import rsi, ema

from blueshift.pipeline import Pipeline
from blueshift.errors import NoFurtherDataError
from blueshift.api import(  schedule_function,
                            date_rules,
                            time_rules,
                            attach_pipeline,
                            pipeline_output,
                            get_datetime,
                       )

def initialize(context):
    context.params = {'lookback':12, 'min_volume':1E7
                      }
    
    schedule_function(strategy, date_rules.month_start(), 
            time_rules.market_close(minutes=1))

    attach_pipeline(make_strategy_pipeline(context), 
            name='strategy_pipeline')

def strategy(context, data):
    generate_signals(context, data)
    rebalance(context,data)

def make_strategy_pipeline(context):
    pipe = Pipeline()

    # Set the volume filter, 126 days is roughly 6 month daily data
    volume_filter = average_volume_filter(126, 1E8)
    
    # compute past returns
    rsi_factor = technical_factor(126, rsi, 14)
    ema20_factor = technical_factor(126, ema, 20)
    ema50_factor = technical_factor(126, ema, 50)
    
    # add to pipelines
    pipe.add(rsi_factor,'rsi')
    pipe.add(ema20_factor,'ema20')
    pipe.add(ema50_factor,'ema50')
    pipe.set_screen(volume_filter)

    return pipe

def generate_signals(context, data):
    try:
        results = pipeline_output('strategy_pipeline')
    except:
        return
    
    # use other columns to print other indicators scanning results
    results = results.sort_values('rsi',ascending=True)
    print('{}{}'.format(get_datetime(),'-'*30))
    print(results.head())
    print(results.tail())

def rebalance(context,data):
    pass
    