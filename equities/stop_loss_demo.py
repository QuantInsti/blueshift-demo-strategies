"""
    Title: Bollinger Band Strategy (NSE)
    Description: This demonstrates a stoploss implementation. The stoploos 
        handling is overlayed upon a long short strategy based on 
        bollinger bands and SMA dual signals in this example. This does 
        not check existing positions and assume all trades are executed.
        In real trading, you must check actual position (instead of 
        target position here) for checking and executing stop losses. This
        may also be extended for take profit strategies similarly. Compare 
        this with the Bollinger band demo strategy.
    Style tags: Systematic Fundamental
    Asset class: Equities, Futures, ETFs and Currencies
    Broker: NSE
"""
import numpy as np
from blueshift_library.technicals.indicators import bollinger_band, ema

from blueshift.finance import commission, slippage
from blueshift.api import(  symbol,
                            order_target_percent,
                            set_commission,
                            set_slippage,
                            get_datetime,
                       )

def initialize(context):
    """
        A function to define things to do at the start of the strategy
    """
    # universe selection
    context.securities = [symbol('NIFTY-I')]

    # define strategy parameters
    context.params = {'indicator_lookback':375,
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.5,
                      'sell_signal_threshold':-0.5,
                      'SMA_period_short':15,
                      'SMA_period_long':60,
                      'BBands_period':300,
                      'trade_freq':5,
                      'leverage':2}

    # variable to control trading frequency
    context.bar_count = 0

    # variables to track signals and target portfolio
    context.signals = dict((security,0) for security in context.securities)
    context.target_position = dict((security,0) for security in context.securities)
    context.entry_price = dict((security,0) for security in context.securities)
    context.entry_side = dict((security,0) for security in context.securities)
    context.stoploss = 0.01 # percentage stoploss

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))


def handle_data(context, data):
    """
        A function to define things to do at every bar
    """
    if check_stop_loss(context, data):
        print('{} got stopped out'.format(get_datetime()))
        return
    
    context.bar_count = context.bar_count + 1
    if context.bar_count < context.params['trade_freq']:
        return

    # time to trade, call the strategy function
    context.bar_count = 0
    run_strategy(context, data)


def run_strategy(context, data):
    """
        A function to define core strategy steps
    """
    generate_signals(context, data)
    generate_target_position(context, data)
    rebalance(context, data)
    calculate_entry_price(context, data)

def rebalance(context,data):
    """
        A function to rebalance - all execution logic goes here
    """
    for security in context.securities:
        order_target_percent(security, context.target_position[security])

def generate_target_position(context, data):
    """
        A function to define target portfolio
    """
    num_secs = len(context.securities)
    weight = round(1.0/num_secs,2)*context.params['leverage']

    for security in context.securities:
        if context.signals[security] > context.params['buy_signal_threshold']:
            context.target_position[security] = weight
        elif context.signals[security] < context.params['sell_signal_threshold']:
            context.target_position[security] = -weight
        else:
            context.target_position[security] = 0


def generate_signals(context, data):
    """
        A function to define define the signal generation
    """
    try:
        price_data = data.history(context.securities, 'close',
            context.params['indicator_lookback'],
            context.params['indicator_freq'])
    except:
        return

    for security in context.securities:
        px = price_data.loc[:,security].values
        context.signals[security] = signal_function(px, context.params)

def signal_function(px, params):
    """
        The main trading logic goes here, called by generate_signals above
    """
    upper, mid, lower = bollinger_band(px,params['BBands_period'])
    if upper - lower:
        return 0
    
    ind2 = ema(px, params['SMA_period_short'])
    ind3 = ema(px, params['SMA_period_long'])
    last_px = px[-1]
    dist_to_upper = 100*(upper - last_px)/(upper - lower)

    if dist_to_upper > 95:
        return -1
    elif dist_to_upper < 5:
        return 1
    elif dist_to_upper > 40 and dist_to_upper < 60 and ind2-ind3 < 0:
        return -1
    elif dist_to_upper > 40 and dist_to_upper < 60 and ind2-ind3 > 0:
        return 1
    else:
        return 0

def calculate_entry_price(context, data):
    # update only if there is a change, i.e. a new entry or exit or a flip
    # in position for the asset. Also reset for exits
    px = data.current(context.securities,'close')

    for security in context.securities:
        if context.entry_price[security] == 0 and \
            context.target_position[security] !=0:
            # we entered a fresh position
            context.entry_price[security] = px[security]
            context.entry_side[security] = np.sign(context.target_position[security])
        elif context.entry_price[security] != 0 and \
            context.target_position[security] == 0:
            # reset for exits
            context.entry_price[security] = 0
            context.entry_side[security] = 0
        elif np.sign(context.target_position[security]) !=\
            context.entry_side[security]:
            # we flipped an existing position
            context.entry_price[security] = px[security]
            context.entry_side[security] = np.sign(context.target_position[security])

def check_stop_loss(context, data):
    px = data.current(context.securities,'close')
    for security in context.securities:
        if context.entry_side[security] == 0:
            continue
        loss = px[security]/context.entry_price[security] - 1
        if context.entry_side[security] == 1 and\
            loss < -context.stoploss:
            # we were long and hit the stoploss
            order_target_percent(security, 0)
            # reset data
            context.entry_price[security] = 0
            context.entry_side[security] = 0
            context.target_position[security] = 0
            return True
        elif context.entry_side[security] == -1 and\
            loss > context.stoploss:
            # we were short and hit the stoploss
            order_target_percent(security, 0)
            # reset data
            context.entry_price[security] = 0
            context.entry_side[security] = 0
            context.target_position[security] = 0
            return True

    return False
