"""
    Title: Intraday Technical Strategies
    Description: This is a long short strategy based on Fibonacci support and resistance.
        Goes with the momentum for levels break-outs, else buys near support and sells
        near resistance if confirmed by ADX
    Style tags: momentum and mean reversion
    Asset class: Equities, Futures, ETFs and Currencies
    Broker: NSE
"""
from blueshift_library.technicals.indicators import fibonacci_support, adx

from blueshift.finance import commission, slippage
from blueshift.api import(  symbol,
                            order_target_percent,
                            set_commission,
                            set_slippage,
                            schedule_function,
                            date_rules,
                            time_rules,
                            cancel_order,
                       )


class Signal:
    STRONG_BUY = 10
    BUY = 1
    STRONG_SELL = -10
    SELL = -1
    NO_SIGNAL = 999
    
    @classmethod
    def get_position_size(cls, signal):
        if signal == cls.STRONG_BUY:
            return 1
        elif signal == cls.BUY:
            return 0.5
        if signal == cls.STRONG_SELL:
            return -1
        elif signal == cls.SELL:
            return -0.5
        return 0

def initialize(context):
    # strategy parameters
    context.params = {'lookback':375,
                      'universe':['NIFTY-I','BANKNIFTY-I'],
                      'ADX_period':120,
                      'leverage':2}
    
    if not context.params['universe']:
        raise ValueError(f'universe not defined.')
    context.universe = [symbol(sym) for sym in context.params['universe']]

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.002, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))
    
    schedule_function(enter_trades, date_rules.every_day(),
                      time_rules.every_nth_minute())
    schedule_function(stop_entry, date_rules.every_day(),
                      time_rules.market_close(hours=2))
    schedule_function(square_off_all, date_rules.every_day(),
                      time_rules.market_close(minutes=30))
    
def before_trading_start(context, data):
    # reset all trackers
    context.entry = True
    context.trade = True
    context.entered = {}
    context.exited = set()

def stop_entry(context, data):
    context.entry = False
    
def square_off_all(context, data):
    for oid in context.open_orders:
        cancel_order(oid)
        
    for asset in context.portfolio.positions:
        order_target_percent(asset, 0)
        
    context.trade = False

def enter_trades(context, data):
    if not context.trade:
        return
    
    cols = ['open','high','low','close']
    lookback = context.params['lookback']
    try:
        ohlc = data.history(context.universe, cols, lookback, '1m')
    except:
        return

    for asset in context.universe:
        px = ohlc.xs(asset)
        if asset not in context.entered:
            check_entry(context, asset, px)
        elif asset not in context.exited:
            check_exit(context, asset, px)
        
def check_entry(context, asset, px):
    if not context.entry or not context.trade:
        return
    
    if asset in context.exited or asset in context.entered:
        return
    
    signal = signal_function(px, context.params)
    if signal == Signal.NO_SIGNAL:
        return
    
    pos = Signal.get_position_size(signal)
    size = pos*context.params['leverage']/len(context.universe)
    order_target_percent(size)
    context.entered[asset]=pos
    
def check_exit(context, asset, px):
    if not context.trade:
        return
    
    if asset not in context.entered or asset in context.exited:
        return
    
    pos = context.entered[asset]
    signal = signal_function(px, context.params)
    if pos > 0 and signal in (Signal.STRONG_SELL, Signal.SELL):
        cancel_and_exit(context, asset)
    elif pos < 0 and signal in (Signal.STRONG_BUY, Signal.BUY):
        cancel_and_exit(context, asset)
            
def cancel_and_exit(context, asset):
    orders = context.open_orders_by_asset(asset)
    for order_id in orders:
        try:
            cancel_order(order_id)
        except:
            pass
            
    positions = context.portfolio.positions
    if asset in positions:
        order_target_percent(asset, 0)
        
    context.exited.add(asset)

def signal_function(px, params):
    lower, upper = fibonacci_support(px.close.values)
    ind2 = adx(px, params['ADX_period'])

    if lower == -1:
        return Signal.STRONG_SELL
    elif upper == -1:
        return Signal.STRONG_BUY
    elif upper > 0.02 and lower > 0 and upper/lower > 3 and ind2 < 20:
        return Signal.SELL
    elif lower > 0.02 and upper > 0 and lower/upper > 3 and ind2 < 20:
        return Signal.BUY
    else:
        return Signal.NO_SIGNAL
