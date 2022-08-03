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
from blueshift_library.pipelines.pipelines import technical_factor
from blueshift_library.technicals.indicators import volatility
from blueshift_library.toolbox.statistical import (
        get_hmm_state, find_imp_points)

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

class Signal:
    BUY = 1
    SELL = -1
    NO_SIGNAL = 0

def initialize(context):
    context.strategy_name = 'Fibonacci Breakout Strategy (Stocks)'
    
    # strategy parameters
    context.params = {'daily_lookback':200,
                      'stocks':[],
                      'intraday_lookback':60,
                      'open':30,
                      'frequency':5,
                      'stoploss':0.005,
                      'takeprofit':None,
                      'num_stocks':5,
                      'universe':100,
                      'order_size':1000}
    
    set_algo_parameters('params') # the attribute of context
    
    context.pipeline = False
    context.universe = []
    if context.params['stocks']:
        stocks = context.params['stocks'].splits(',')
        context.universe = [symbol(s) for s in stocks]
    else:
        try:
            context.params['universe'] = int(context.params['universe'])
            assert context.params['universe'] <= 500
            assert context.params['universe'] >= 50
        except:
            msg = 'universe must be an integer between 50 and 500.'
            raise ValueError(msg)
        try:
            context.params['num_stocks'] = int(context.params['num_stocks'])
            assert context.params['num_stocks'] <= 10
            assert context.params['num_stocks'] >= 1
        except:
            msg = 'num_stocks must be an integer between 1 and 10.'
            raise ValueError(msg)
        context.pipeline = True
    try:
        context.params['intraday_lookback'] = int(context.params['intraday_lookback'])
        assert context.params['intraday_lookback'] <= 120
        assert context.params['intraday_lookback'] >= 15
    except:
        msg = 'intraday_lookback must be an integer between 15 and 120 (minutes).'
        raise ValueError(msg)
    try:
        context.params['daily_lookback'] = int(context.params['daily_lookback'])
        assert context.params['daily_lookback'] <= 200
        assert context.params['daily_lookback'] >= 60
    except:
        msg = 'daily_lookback must be an integer between 60 and 200 (days).'
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
        
    try:
        assert context.params['open'] == int(context.params['open'])
        assert context.params['open'] >= 30
        assert context.params['open'] <= 60
    except:
        msg = 'open must be integer and greater than or equal to '
        msg += '30 and less than or equal to 60 (minutes).'
        raise ValueError(msg)
    t = context.params['open']
    
    try:
        assert context.params['frequency'] == int(context.params['frequency'])
        assert context.params['frequency'] >= 1
        assert context.params['frequency'] <= 60
    except:
        msg = 'open must be integer and greater than or equal to '
        msg += '1 and less than or equal to 60 (minutes).'
        raise ValueError(msg)
    f = context.params['frequency']
    
    n = len(context.universe) if context.universe else context.params['num_stocks']
    required = n*context.params['order_size']
    capital = context.portfolio.starting_cash
    if capital < required:
        msg = f'Required capital is {required}, alloted {capital}, '
        msg += f'please add more capital or reduce number of stocks.'
        raise ValueError(msg)
        
    context.intraday_lookback = context.params['intraday_lookback']
        
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
    
    schedule_function(opening_range, date_rules.every_day(),
                      time_rules.market_open(t))
    schedule_function(strategy, date_rules.every_day(),
                      time_rules.every_nth_minute(f))
    schedule_function(stop_entry, date_rules.every_day(),
                      time_rules.market_open(hours=1, minutes=30))
    schedule_function(square_off_all, date_rules.every_day(),
                      time_rules.market_close(minutes=30))
    
    if context.pipeline:
        attach_pipeline(make_strategy_pipeline(context), 
            name='strategy_pipeline')
    
    context.benchmark = symbol('NIFTY')
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
    vol = technical_factor(lookback, volatility, 1)
    pipe.add(vol,'vol')
    pipe.set_screen(dollar_volume_filter)
    return pipe

def generate_universe(context, data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        context.universe = []
        return
    
    n = context.params['num_stocks']
    candidates = pipeline_results.dropna()
    candidates = candidates.vol.sort_values()
    size = int(len(candidates))
    
    if size == 0:
        print(f'{get_datetime()}, no stocks passed filterting criteria.')
        context.universe = []
        return
        
    if size < n:
        print(f'{get_datetime()}, only {size} stocks passed filterting criteria.')
    
    context.universe = candidates[-n:].index.tolist()
    
def before_trading_start(context, data):
    # reset all trackers
    context.entry = False
    context.trade = False
    context.entered = set()
    context.exited = set()
    context.regime = {}
    context.days = {}
    context.prev = {}
    
    if context.pipeline:
        generate_universe(context, data)
        
    cols = ['high','low','close']
    lookback = context.params['daily_lookback']
    prices = data.history(context.universe, cols, lookback, '1d')
    for asset in context.universe:
        px = prices.xs(asset)
        context.regime[asset] = get_hmm_state(px.close)[-1]
        high, low, close = px.high[-1], px.low[-1], px.close[-1]
        context.prev[asset] = (high, low, close)
        
def opening_range(context, data):
    cols = ['high','low','close']
    lookback = context.params['open']
    prices = data.history(context.universe, cols, lookback, '1m')
    
    for asset in context.universe:
        px = prices.xs(asset)
        px = px[px.index.date == get_datetime().date()]
        high, low, close = px.high.max(), px.low.min(), px.close[-1]
        context.days[asset] = (high, low, close)
        
    context.trading = True
    context.entry = True
    print(f'{get_datetime()}:opening range {context.days}')

def stop_entry(context, data):
    context.entry = False
    
def square_off_all(context, data):
    for oid in context.open_orders:
        cancel_order(oid)
        
    square_off()
    context.trade = False

def strategy(context, data):
    if not context.entry:
        return
    if len(context.universe) == len(context.entered):
        return
    
    if not context.universe:
        return
    
    print(f'{get_datetime()}: running strategy')
    prices = data.current(context.universe,'close')
    for asset in context.universe:
        if asset not in context.entered:
            check_entry(context, asset, prices[asset])
        
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
    days_high, days_low, days_close = context.days[asset]
    last_high, last_low, last_close = context.prev[asset]   
    regime = context.regime[asset]
    
    if days_low > last_high and px > days_high and regime == 2:
        return Signal.BUY
    elif days_high < last_low and px < days_low and regime == 0:
        return Signal.SELL
    else:
        print(f'{get_datetime()}:{asset} got {px}')
        return Signal.NO_SIGNAL