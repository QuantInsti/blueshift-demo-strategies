"""
    Title: Relative Strength Index (RSI) Strategy (Forex)
    Description: This is a long short strategy based on RSI and moving average
        dual signals. We also square off all positions at the end of the
        day to avoid any roll-over costs. The trade size is fixed - 
        mini lotsize (1000) multiplied by a leverage. The leverage is a 
        parameter, defaults to 1. Minimum capital 1000.
    Style tags: Momentum, Mean Reversion
    Asset class: Equities, Futures, ETFs and Currencies
    Dataset: FX Minute
"""
from blueshift.library.technicals.indicators import rsi, ema

from blueshift.api import(    symbol,
                            order_target,
                            schedule_function,
                            date_rules,
                            time_rules,
                            set_account_currency,
                            square_off,
                       )

def initialize(context):
    """
        A function to define things to do at the start of the strategy
    """
    # set the account currency, only valid for backtests
    set_account_currency("USD")

    # lot-size (mini-lot for most brokers)
    context.lot_size = 1000

    # universe selection
    context.securities = [
                               symbol('AUD/USD'),
                               symbol('EUR/CHF'),
                               symbol('EUR/JPY'),
                               symbol('EUR/USD'),
                               symbol('GBP/USD'),
                               symbol('NZD/USD'),
                               symbol('USD/CAD'),
                               symbol('USD/CHF'),
                               symbol('USD/JPY'),
                             ]

    # define strategy parameters
    context.params = {'indicator_lookback':375,
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.5,
                      'sell_signal_threshold':-0.5,
                      'SMA_period_short':15,
                      'SMA_period_long':60,
                      'RSI_period':60,
                      'trade_freq':30,
                      'leverage':1,
                      'pip_cost':0.00003}

    # variable to control trading frequency
    context.bar_count = 0
    context.trading_hours = False

    # variables to track signals and target portfolio
    context.signals = dict((security,0) for security in context.securities)
    context.target_position = dict((security,0) for security in context.securities)

    # set a timeout for trading
    schedule_function(stop_trading,
                    date_rules.every_day(),
                    time_rules.market_close(hours=0, minutes=31))
    # call square off to zero out positions 30 minutes before close.
    schedule_function(daily_square_off,
                    date_rules.every_day(),
                    time_rules.market_close(hours=0, minutes=30))


def before_trading_start(context, data):
    """ set flag to true for trading. """
    context.trading_hours = True

def stop_trading(context, data):
    """ stop trading and prepare to square off."""
    context.trading_hours = False

def daily_square_off(context, data):
    """ square off all positions at the end of day."""
    context.trading_hours = False
    square_off()

def handle_data(context, data):
    """
        A function to define things to do at every bar
    """
    if context.trading_hours == False:
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

def rebalance(context,data):
    """
        A function to rebalance - all execution logic goes here
    """
    for security in context.securities:
        order_target(security, context.target_position[security])

def generate_target_position(context, data):
    """
        A function to define target portfolio
    """
    weight = context.lot_size*context.params['leverage']

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
        return -1
    elif ind1 < 30 and ind2-ind3 <0:
        return 1
    else:
        return 0
