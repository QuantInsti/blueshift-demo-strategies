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
    Asset class: Equities, Futures and ETFs.
    Broker: NSE
"""
import talib as ta

from blueshift.finance import commission, slippage
from blueshift.api import(  symbol,
                            order_target_percent,
                            set_commission,
                            set_slippage,
                            schedule_function,
                            date_rules,
                            time_rules,
                            cancel_order,
                            set_stoploss,
                            set_takeprofit,
                       )


class Signal:
    BUY = 1
    SELL = -1
    NO_SIGNAL = 999
    
    @classmethod
    def get_position_size(cls, signal):
        if signal == cls.BUY:
            return 0.5
        elif signal == cls.SELL:
            return -0.5
        return 0

def initialize(context):
    # strategy parameters
    context.params = {'daily_lookback':20,
                      'universe':'NIFTY-I,BANKNIFTY-I',
                      'stoploss':0.005,
                      'takeprofit':None,
                      'short_sma':10,
                      'long_sma':30,
                      'leverage':2}
    
    if not context.params['universe']:
        raise ValueError(f'universe not defined.')
    context.params['universe'] = context.params['universe'].split(',')
    if len(context.params['universe']) > 10:
        raise ValueError(f'universe can be maximum 10 instruments.')
    
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
        
    context.universe = [symbol(sym) for sym in context.params['universe']]

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.002, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))
    
    schedule_function(strategy, date_rules.every_day(),
                      time_rules.every_nth_minute())
    schedule_function(stop_entry, date_rules.every_day(),
                      time_rules.market_close(hours=2))
    schedule_function(square_off_all, date_rules.every_day(),
                      time_rules.market_close(minutes=30))
    
def generate_supports(context, data):
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
    context.entered = {}
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
        order_target_percent(asset, 0)
        
    context.trade = False

def strategy(context, data):
    if not context.trade:
        return
    
    cols = ['close','high','low','volume']
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
    
    pos = Signal.get_position_size(signal)
    size = pos*context.params['leverage']/len(context.universe)
    order_target_percent(asset, size)
    context.entered[asset]=pos
    
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
    typical = 0.33*(px.close[-1] + px.high[-1] + px.low[-1])
    t = context.params['short_sma']
    T = context.params['long_sma']
    volume_signal = ta.SMA(px.volume, t)[-1] > ta.SMA(px.volume, T)[-1]
    
    if typical > context.supports[asset][-1] and volume_signal:
        # break-out on the upside
        return Signal.BUY
    elif typical < context.supports[asset][0] and volume_signal:
        # break-out on the downside
        return Signal.SELL
    else:
        return Signal.NO_SIGNAL