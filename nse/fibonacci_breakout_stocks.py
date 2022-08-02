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
    Asset class: Index Futures.
    Dataset: NSE
    Risk: High
    Minimum Capital: 300,000
"""
import numpy as np
import talib as ta

from blueshift.finance import commission, slippage
from blueshift.api import(  symbol,
                            order_target,
                            set_commission,
                            set_slippage,
                            schedule_function,
                            date_rules,
                            time_rules,
                            cancel_order,
                            attach_pipeline,
                            pipeline_output,
                            get_datetime,
                            set_long_only,
                            set_stoploss,
                            set_takeprofit,
                            set_algo_parameters,
                       )

from blueshift.pipeline import Pipeline
from blueshift.errors import NoFurtherDataError
from blueshift.pipeline.factors import AverageDollarVolume
from blueshift.pipeline import CustomFactor
from blueshift.pipeline.data import EquityPricing

class Signal:
    STRONG_BUY = 10
    BUY = 1
    SELL = -1
    STRONG_SELL = -10
    NO_SIGNAL = 999
    
def atr_factor(window_length, lookback):
    class ATR(CustomFactor):
        inputs = [EquityPricing.close, EquityPricing.high, EquityPricing.low]
        def compute(self,today,assets,out,close,high,low):
            for i in range(close.shape[1]):
                try:
                    atr = ta.ATR(
                            high[:,i],low[:,i],close[:,i],lookback)
                    out[i] = atr[-1]
                except:
                    out[i] = np.nan
    return ATR(window_length = window_length)

def initialize(context):
    context.strategy_name = 'Fibonacci Breakout Strategy (Stocks)'
    
    # strategy parameters
    context.params = {'daily_lookback':20,
                      'stocks':[],
                      'stoploss':0.005,
                      'takeprofit':None,
                      'num_stocks':5,
                      'universe':100,
                      'order_size':1000}
    
    set_algo_parameters('params') # the attribute of context
    
    context.pipeline = False
    if not context.params['stocks']:
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
        context.universe = []
        context.pipeline = True
    else:
        context.universe = context.params['stocks'].split(',')
        context.universe = [symbol(s) for s in context.universe]
        
    n = len(context.universe) if context.universe else context.params['num_stocks']
    required = n*context.params['order_size']
    capital = context.portfolio.starting_cash
    if capital < required:
        msg = f'Required capital is {required}, alloted {capital}, '
        msg += f'please add more capital or reduce number of stocks.'
        raise ValueError(msg)
    
    try:
        assert context.params['daily_lookback'] == int(context.params['daily_lookback'])
        assert context.params['daily_lookback'] > 10
        assert context.params['daily_lookback'] <= 60
    except:
        msg = 'daily lookback must be integer and greater than 10 and '
        msg += 'less than or equal to 60.'
        raise ValueError(msg)
        
    context.intraday_lookback = 10 # we use last 5 bars only
        
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

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.002, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))
    
    schedule_function(strategy, date_rules.every_day(),
                      time_rules.every_nth_minute())
    schedule_function(stop_entry, date_rules.every_day(),
                      time_rules.market_close(hours=2))
    schedule_function(square_off_all, date_rules.every_day(),
                      time_rules.market_close(minutes=30))
    
    if context.pipeline:
        attach_pipeline(make_strategy_pipeline(context), 
            name='strategy_pipeline')
    
    set_long_only()
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
    atr = atr_factor(2*lookback, lookback)
    pipe.add(atr,'atr')
    pipe.set_screen(dollar_volume_filter)
    return pipe

def generate_pipeline_universe(context, data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        context.universe = []
        return
    
    n = context.params['num_stocks']
    candidates = pipeline_results.dropna()
    candidates = candidates.atr.sort_values()
    size = len(candidates)
    
    if size == 0:
        print(f'{get_datetime()}, no stocks passed filterting criteria.')
        context.universe = []
        return
        
    if size < n:
        print(f'{get_datetime()}, only {size} stocks passed filterting criteria.')
        
    # choose stocks with highest true ranges
    context.universe = candidates[-n:].index.tolist()
    
def generate_supports(context, data):
    if context.pipeline:
        generate_pipeline_universe(context, data)
        
    if not context.universe:
        return
    
    cols = ['close']
    lookback = context.params['daily_lookback']
    ohlc = data.history(context.universe, cols, lookback, '1d')
    
    for asset in context.universe:
        px = ohlc.xs(asset).close
        context.supports[asset] = [min(px) + l*(max(px) - min(px)) \
                          for l in [0,0.236,0.382,0.5,0.618,1]]
    
def before_trading_start(context, data):
    # reset all trackers
    context.entry = True
    context.trade = True
    context.entered = set()
    context.exited = set()
    context.supports = {}
    context.signal = {}
    generate_supports(context, data)

def stop_entry(context, data):
    context.entry = False
    
def square_off_all(context, data):
    for oid in context.open_orders:
        cancel_order(oid)
        
    for asset in context.portfolio.positions:
        order_target(asset, 0)
        
    context.trade = False

def strategy(context, data):
    if not context.trade or not context.universe:
        return
    
    cols = ['close','high','low']
    ohlc = data.history(
            context.universe, cols, context.intraday_lookback, '1m')

    for asset in context.universe:
        px = ohlc.xs[asset]
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
    
    size = context.params['order_size']
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
    last, prev = px.close[-1], px.close[-2]
    high, low = np.nanmax(px.high[-5:]), np.nanmin(px.high[-5:])
    s1, s2 = context.supports[asset][0], context.supports[asset][1]
    r1, r2 = context.supports[asset][-2], context.supports[asset][-1]
    
    # check if we have a pullback from the supports
    if low < s1 and last > s1 and prev > s1:
        # pullback from low
        return Signal.STRONG_BUY
    elif high > r2 and last < r2 and prev < r2:
        # pullback from high
        return Signal.STRONG_SELL
    elif low < s2 and last > s2 and prev > s2:
        pass
    elif high > r1 and last < r1 and prev < r1:
        pass
    else:
        return Signal.NO_SIGNAL