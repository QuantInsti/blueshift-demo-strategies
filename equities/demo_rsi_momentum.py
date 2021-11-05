"""
    Title: Intraday Technical Strategies
    Description: This is a long short strategy based on RSI and moving average
        dual signals
    Style tags: Momentum, Mean Reversion
    Asset class: Equities, Futures, ETFs and Currencies
    Broker: NSE
"""
from blueshift_library.technicals.indicators import rsi, ema

from blueshift.finance import commission, slippage
from blueshift.api import(    symbol,
                            order_target_percent,
                            set_commission,
                            set_slippage,
                       )

def initialize(context):
    """
        A function to define things to do at the start of the strategy
    """
    # universe selection
    context.securities = [symbol('NIFTY-I'),symbol('BANKNIFTY-I')]

    # define strategy parameters
    context.params = {'indicator_lookback':375,
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.5,
                      'sell_signal_threshold':-0.5,
                      'SMA_period_short':15,
                      'SMA_period_long':60,
                      'RSI_period':60,
                      'trade_freq':5,
                      'leverage':2}

    # variable to control trading frequency
    context.bar_count = 0

    # variables to track signals and target portfolio
    context.signals = dict((security,0) for security in context.securities)
    context.target_position = dict((security,0) for security in context.securities)

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))


def handle_data(context, data):
    """
        A function to define things to do at every bar
    """
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
            context.params['indicator_lookback'], context.params['indicator_freq'])
    except:
        return

    for security in context.securities:
        px = price_data.loc[:,security].values
        context.signals[security] = signal_function(px, context.params)

def signal_function(px, params):
    """
        The main trading logic goes here, called by generate_signals above
    """
    ind1 = rsi(px, params['RSI_period'])
    ind2 = ema(px, params['SMA_period_short'])
    ind3 = ema(px, params['SMA_period_long'])

    if ind1 > 60 and ind2-ind3 > 0:
        return 1
    elif ind1 < 30 and ind2-ind3 <0:
        return -1
    else:
        return 0
