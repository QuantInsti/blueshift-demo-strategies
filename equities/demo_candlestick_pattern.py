"""
    Title: Intraday Technical Strategies
    Description: This is a long short strategy based on Doji pattern and Bollinger Bands
        Bollinger band. If we have a Doji near the upper band we go
        with the momentum.
    Style tags: momentum and mean reversion
    Asset class: Equities, Futures, ETFs and Currencies
    Broker: NSE
"""
from blueshift_library.technicals.indicators import bollinger_band, doji

from blueshift.finance import commission, slippage
from blueshift.api import(  symbol,
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
                      'ROC_period_short':30,
                      'ROC_period_long':120,
                      'BBands_period':300,
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
    '''
        A function to define target portfolio
    '''
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
        price_data = data.history(context.securities, ['open','high','low','close'],
            context.params['indicator_lookback'], context.params['indicator_freq'])
    except:
        return

    for security in context.securities:
        px = price_data.xs(security)
        context.signals[security] = signal_function(px, context.params,
            context.signals[security])

def signal_function(px, params, last_signal):
    """
        The main trading logic goes here, called by generate_signals above
    """
    ind1 = doji(px)
    upper, mid, lower = bollinger_band(px.close.values,params['BBands_period'])
    if upper - lower == 0:
        return 0
    
    last_px = px.close.values[-1]
    dist_to_upper = 100*(upper - last_px)/(upper - lower)

    if ind1 > 0 and dist_to_upper < 30:
        return 1
    elif ind1 > 0 and dist_to_upper > 70:
        return -1
    else:
        return last_signal
