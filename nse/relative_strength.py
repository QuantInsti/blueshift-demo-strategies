"""
    Title: Fibonacci Breakout Strategy (Intraday).
    Description: This is a long short strategy based on Fibonacci support 
        and resistance. The supports and resistance levels are established
        at before the market opens. During the market hours, if the 
        typical price crosses either the support or the resistance level,
        we position for a breakout, with optional stoploss or takeprofit 
        orders. Entry is allowed till 2 hours before the market closes. 
        All positions are squared off 30 minutes before the market closes.
    Style tags: momentum and breakout.
    Asset class: Equties and ETFs.
    Dataset: NSE
    Risk: High
    Minimum Capital: 300,000
"""
import numpy as np

from blueshift.api import(  symbol,
                            order_target,
                            schedule_function,
                            date_rules,
                            time_rules,
                            cancel_order,
                            attach_pipeline,
                            pipeline_output,
                            get_datetime,
                            set_stoploss,
                            set_takeprofit,
                            set_algo_parameters,
                            square_off
                       )

from blueshift.pipeline import Pipeline
from blueshift.errors import NoFurtherDataError
from blueshift.pipeline.factors import AverageDollarVolume
from blueshift.pipeline import CustomFactor
from blueshift.pipeline.data import EquityPricing
from blueshift_library.technicals.indicators import rsi

class Signal:
    BUY = 1
    SELL = -1
    NO_SIGNAL = 0

def rs_factor(window_length):
    class Move(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close):
            out[:] = 100*(close[-1]/close[0])
    return Move(window_length = window_length)

def initialize(context):
    context.strategy_name = 'Fibonacci Breakout Strategy (Stocks)'
    
    # strategy parameters
    context.params = {'daily_lookback':60,
                      'rsi_period':60,
                      'stoploss':0.005,
                      'takeprofit':None,
                      'num_stocks':5,
                      'universe':100,
                      'order_size':1000}
    
    set_algo_parameters('params') # the attribute of context
    
    try:
        context.params['universe'] = int(context.params['universe'])
        assert context.params['universe'] <= 500
        assert context.params['universe'] >= 50
    except:
        msg = 'universe must be an integer between 50 and 500.'
        raise ValueError(msg)
    try:
        context.params['num_stocks'] = int(context.params['num_stocks'])
        assert context.params['num_stocks'] <= 20
        assert context.params['num_stocks'] >= 2
    except:
        msg = 'num_stocks must be an integer between 2 and 20.'
        raise ValueError(msg)
    try:
        context.params['rsi_period'] = int(context.params['rsi_period'])
        assert context.params['rsi_period'] <= 120
        assert context.params['rsi_period'] >= 15
    except:
        msg = 'rsi_period must be an integer between 15 and 120 (minutes).'
        raise ValueError(msg)
    try:
        context.params['daily_lookback'] = int(context.params['daily_lookback'])
        assert context.params['daily_lookback'] <= 200
        assert context.params['daily_lookback'] >= 20
    except:
        msg = 'daily_lookback must be an integer between 20 and 200 (days).'
        raise ValueError(msg)
        
    if context.params['stoploss']:
        try:
            sl = float(context.params['stoploss'])
            if sl < 0 or sl > 0.1:raise ValueError()
        except:
            'stoploss must be a fraction between 0 to 0.10'
            raise ValueError(msg)
    else:
        context.params['stoploss'] = None
            
    if context.params['takeprofit']:
        try:
            tp = float(context.params['takeprofit'])
            if tp < 0.005 or tp > 0.1:raise ValueError()
        except:
            'takeprofit must be a fraction between 0.005 to 0.10'
            raise ValueError(msg)
    else:
        context.params['takeprofit'] = None
        
    context.long_universe = []
    context.short_universe = []
        
    n = 2*context.params['num_stocks']
    required = n*context.params['order_size']
    capital = context.portfolio.starting_cash
    if capital < required:
        msg = f'Required capital is {required}, alloted {capital}, '
        msg += f'please add more capital or reduce number of stocks.'
        raise ValueError(msg)
        
    context.intraday_lookback = 2*context.params['rsi_period']
        
    if context.params['stoploss']:
        try:
            sl = float(context.params['stoploss'])
            if sl < 0 or sl > 0.1:raise ValueError()
        except:
            'stoploss must be a fraction between 0 to 0.10'
            raise ValueError(msg)
    else:
        context.params['stoploss'] = None
            
    if context.params['takeprofit']:
        try:
            tp = float(context.params['takeprofit'])
            if tp < 0.005 or tp > 0.1:raise ValueError()
        except:
            'takeprofit must be a fraction between 0.005 to 0.10'
            raise ValueError(msg)
    else:
        context.params['takeprofit'] = None
    
    schedule_function(strategy, date_rules.every_day(),
                      time_rules.every_nth_minute())
    schedule_function(stop_entry, date_rules.every_day(),
                      time_rules.market_close(hours=2))
    schedule_function(square_off_all, date_rules.every_day(),
                      time_rules.market_close(minutes=30))
    
    if context.pipeline:
        attach_pipeline(make_strategy_pipeline(context), 
            name='strategy_pipeline')
    
    context.benchmark = symbol('NIFTY50')
    msg = f'Starting strategy {context.strategy_name} '
    msg += f'with parameters {context.params}'
    print(msg)
    
def make_strategy_pipeline(context):
    pipe = Pipeline()

    lookback = context.params['daily_lookback']
    top_n = context.params['universe']
    dollar_volume_filter = AverageDollarVolume(
            window_length=lookback).top(top_n)
    
    # compute atr
    move = rs_factor(window_length=lookback)
    pipe.add(move,'move')
    pipe.set_screen(dollar_volume_filter)
    return pipe

def generate_universe(context, data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        context.long_universe = []
        context.short_universe = []
        return
    
    n = context.params['num_stocks']
    lookback = context.params['daily_lookback']
    benchmark = data.history(context.benchmark,'close',lookback,'1d')
    move = 100*(benchmark[-1]/benchmark[0])
    
    candidates = pipeline_results.dropna()
    metric = candidates.move/move
    candidates = metric.sort_values()
    size = int(len(candidates)/2)
    
    if size == 0:
        print(f'{get_datetime()}, no stocks passed filterting criteria.')
        context.long_universe = []
        context.short_universe = []
        return
        
    if size < n:
        print(f'{get_datetime()}, only {size} stocks passed filterting criteria.')
    
    context.long_universe = candidates[-size:].index.tolist()
    context.short_universe = candidates[:size].index.tolist()
    
def before_trading_start(context, data):
    # reset all trackers
    context.entry = True
    context.trade = True
    context.entered = set()
    context.exited = set()
    context.supports = {}
    context.signal = {}
    generate_universe(context, data)

def stop_entry(context, data):
    context.entry = False
    
def square_off_all(context, data):
    for oid in context.open_orders:
        cancel_order(oid)
        
    square_off()
    context.trade = False

def strategy(context, data):
    if not context.trade:
        return
    
    if not context.long_universe and not context.short_universe:
        return
    
    universe = list(set(context.long_universe + context.short_universe))
    cols = 'close'
    ohlc = data.history(universe, cols, context.intraday_lookback, '1m')

    for asset in universe:
        px = ohlc[asset]
        if asset not in context.entered:
            check_entry(context, asset, px)
        
def check_entry(context, asset, px):
    if not context.entry or not context.trade:
        return
    
    if asset in context.exited or asset in context.entered:
        return
    
    signal = signal_function(context, asset, px)
    if signal == Signal.NO_SIGNAL:
        return
    
    size = context.params['order_size']*signal
    order_target(asset, size)
    context.entered.add(asset)
    
    if context.params['stoploss']:
        set_stoploss(
                asset, 'PERCENT', context.params['stoploss'], 
                trailing=False, on_stoploss=on_exit)
    if context.params['takeprofit']:
        set_takeprofit(asset, 'PERCENT', context.params['takeprofit'],
                       on_takeprofit=on_exit)
        
def on_exit(context, asset):
    context.exited.add(asset)

def signal_function(context, asset, px):
    sig = rsi(px, context.params['rsi_period'])
    
    if sig > 65 and asset in context.long_universe:
        return Signal.BUY
    elif sig < 35 and asset in context.short_universe:
        return Signal.SELL
    else:
        return Signal.NO_SIGNAL