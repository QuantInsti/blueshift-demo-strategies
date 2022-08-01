"""
    Title: Intraday Technical Strategies
    Description: This is a long short strategy based on Fibonacci support and resistance.
        Goes with the momentum for levels break-outs, else buys near support and sells
        near resistance if confirmed by ADX
    Style tags: momentum and mean reversion
    Asset class: Equities, Futures, ETFs and Currencies
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
                      'intraday_lookback':60,
                      'universe':['NIFTY-I','BANKNIFTY-I'],
                      'stoploss':0.005,
                      'leverage':2}
    
    if not context.params['universe']:
        raise ValueError(f'universe not defined.')
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
    set_stoploss(
            asset, 'PERCENT', context.params['stoploss'], trailing=False, 
            on_stoploss=on_exit)
        
def on_exit(context, asset):
    context.exited.add(asset)

def signal_function(context, asset, px):
    typical = 0.33*(px.close[-1] + px.high[-1] + px.low[-1])
    volume_signal = ta.SMA(px.volume, 10)[-1] > ta.SMA(px.volume, 30)[-1]
    
    if typical > context.supports[asset][-1] and volume_signal:
        # break-out on the upside
        return Signal.BUY
    elif typical < context.supports[asset][0] and volume_signal:
        # break-out on the downside
        return Signal.SELL
    else:
        return Signal.NO_SIGNAL