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
from blueshift_library.technicals.indicators import ema

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
        assert context.params['frequency'] == int(context.params['frequency'])
        assert context.params['frequency'] >= 1
        assert context.params['frequency'] <= 30
    except:
        msg = 'frequency must be integer and greater than or equal to '
        msg += '10 and less than or equal to 30.'
        raise ValueError(msg)
    t = context.params['frequency']
    
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
    
    schedule_function(strategy, date_rules.every_day(),
                      time_rules.every_nth_minute(t))
    schedule_function(stop_entry, date_rules.every_day(),
                      time_rules.market_close(hours=2))
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
    context.entry = True
    context.trade = True
    context.entered = set()
    context.exited = set()
    context.regime = {}
    context.support = {}
    context.resistance = {}
    context.signal = {}
    
    if context.pipeline:
        generate_universe(context, data)
        
    lookback = context.params['daily_lookback']
    prices = data.history(context.universe, 'close', lookback, '1d')
    for asset in context.universe:
        px = prices[asset]
        context.regime[asset] = get_hmm_state(px)[-1]
        R = 1.02 + (lookback-60)*(1.05-1.02)/(200-60)
        _, _, points = find_imp_points(px, R=R)
        support = points[points.sign==-1]
        resistance = points[points.sign==1]
        context.support[asset] = sorted(support.value.tail(5).tolist())
        context.resistance[asset] = sorted(resistance.value.tail(5).tolist())

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
    if len(context.universe) == len(context.entered):
        return
    
    if not context.universe:
        return
    
    cols = 'close'
    ohlc = data.history(context.universe, cols, context.intraday_lookback, '1m')

    for asset in context.universe:
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
    last = px[-1]
    sig1 = ema(px, context.intraday_lookback)
    sig2 = ema(px, context.intraday_lookback/2)
    
    if len(context.support[asset]) > 1:
        support = context.support[asset][1]
    elif len(context.support[asset])== 1:
        support = context.support[asset][0]
    else:
        support = last
        
    if len(context.resistance[asset]) > 1:
        resistance = context.resistance[asset][-2]
    elif len(context.resistance[asset])== 1:
        resistance = context.resistance[asset][-1]
    else:
        resistance = last
    
    regime = context.regime[asset]
    momentum = 100*(sig2/sig1-1)
    
    if momentum > 1.005 and regime==2 and last > support:
        return Signal.BUY
    elif momentum < 0.995 and regime==0 and last < resistance:
        return Signal.SELL
    else:
        return Signal.NO_SIGNAL