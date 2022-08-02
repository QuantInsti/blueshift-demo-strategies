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
"""
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
                            set_stoploss,
                            set_takeprofit,
                            set_algo_parameters,
                       )


class Signal:
    BUY = 1
    SELL = -1
    NO_SIGNAL = 999

def initialize(context):
    context.strategy_name = 'Fibonacci Breakout Strategy (Futures)'
    
    # strategy parameters
    context.params = {'daily_lookback':20,
                      'nifty':True,
                      'banknifty':True,
                      'stoploss':0.005,
                      'takeprofit':None,
                      'short_sma':10,
                      'long_sma':30,
                      'lotsize':1}
    
    set_algo_parameters('params') # the attribute of context
    
    context.params['universe'] = {}
    if context.params['nifty']:
        context.params['universe'][symbol('NIFTY-I')] = 50 # lotsize
    if context.params['banknifty']:
        context.params['universe'][symbol('BANKNIFTY-I')] = 25 # lotsize
    if not context.params['universe']:
        raise ValueError(f'must choose atleast one of nifty or banknifty.')
    
    
    try:
        assert context.params['daily_lookback'] == int(context.params['daily_lookback'])
        assert context.params['daily_lookback'] > 10
        assert context.params['daily_lookback'] <= 200
    except:
        msg = 'daily lookback must be integer and greater than 10 and '
        msg += 'less than or equal to 200.'
        raise ValueError(msg)
        
    try:
        assert context.params['short_sma'] == int(context.params['short_sma'])
        assert context.params['long_sma'] == int(context.params['long_sma'])
        assert context.params['long_sma'] > context.params['short_sma']
    except:
        msg = 'volume moving average lookbacks must be integers and '
        msg += 'long term lookback must be greater than short term lookback.'
        raise ValueError(msg)
    else:
        context.params['intraday_lookback'] = context.params['long_sma']+10
        
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
        
    context.universe = list(context.params['universe'].keys())

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.002, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))
    
    schedule_function(strategy, date_rules.every_day(),
                      time_rules.every_nth_minute())
    schedule_function(stop_entry, date_rules.every_day(),
                      time_rules.market_close(hours=2))
    schedule_function(square_off_all, date_rules.every_day(),
                      time_rules.market_close(minutes=30))
    
    context.capital_checked = False
    
def generate_supports(context, data):
    cols = ['close']
    lookback = context.params['daily_lookback']
    ohlc = data.history(context.universe, cols, lookback, '1d')
    
    for asset in context.universe:
        px = ohlc.xs(asset).close
        context.supports[asset] = [min(px) + l*(max(px) - min(px)) \
                          for l in [0,0.236,0.382,0.5,0.618,1]]
    
def before_trading_start(context, data):
    if not context.capital_checked:
        assets = list(context.universe.keys())
        prices = data.current(assets, 'close')
        lotsize = context.params['lotsize']
        required = 0
        for asset in assets:
            required += context.universe[asset]*prices[asset]*lotsize
        capital = context.portfolio.starting_cash
        if capital < required:
            msg = f'Required capital is {required}, alloted {capital}, '
            msg += f'please add more capital and try again.'
            raise ValueError(msg)
        msg = f'Starting strategy {context.strategy_name} '
        msg += f'with parameters {context.params}'
        print(msg)
        context.capital_checked = True
        
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
    if not context.trade:
        return
    
    #cols = ['close','high','low','volume']
    cols = ['close','volume']
    lookback = context.params['intraday_lookback']
    ohlc = data.history(context.universe, cols, lookback, '1m')

    for asset in context.universe:
        px = ohlc.xs(asset)
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
    
    lotsize = context.params['universe'][asset]
    size = lotsize*context.params['lotsize']
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
    #last = 0.33*(px.close[-1] + px.high[-1] + px.low[-1])
    last = px.close[-1]
    t = context.params['short_sma']
    T = context.params['long_sma']
    volume_signal = ta.SMA(px.volume, t)[-1] > ta.SMA(px.volume, T)[-1]
    
    if last > context.supports[asset][-1] and volume_signal:
        # break-out on the upside
        return Signal.BUY
    elif last < context.supports[asset][0] and volume_signal:
        # break-out on the downside
        return Signal.SELL
    else:
        return Signal.NO_SIGNAL