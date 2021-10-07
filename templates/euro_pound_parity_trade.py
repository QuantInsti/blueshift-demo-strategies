"""
    Title: Cross-Channel Parity (Euro and Pound)
    Description: This is a sample Pairs Trading strategy with the 
        Euro and Sterling Pound (the cross-channel spread). This should
        have worked till 2016 (Brexit!!), and many be in future. 
        Minimum capital 10,000.
    Style tags: Mean-reversion, Stat-Arb
    Asset class: Equities, Futures, ETFs, Currencies and Commodities
    Dataset: FX Minute
"""
import numpy as np
from blueshift_library.utils.utils import z_score, hedge_ratio, cancel_all_open_orders
from blueshift_library.utils.utils import square_off

from blueshift.api import(  symbol,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            set_account_currency
                       )

def initialize(context):
    """
        function to define things to do at the start of the strategy
    """
    # set the account currency, only valid for backtests
    set_account_currency("USD")
    
    # trading pound parity!
    # this should work after the European sovereign crisis settled down
    # and before the Brexit noise started (2012-2015)
    context.x = symbol('GBP/USD')
    context.y = symbol('EUR/USD')
    context.leverage = 5
    context.signal = 0

    # Trade entry and exit when the z_score is +/- entry_z_score and exit_z_score respectively
    context.entry_z_score = 2.0
    context.exit_z_score = 0.5

    # Lookback window
    context.lookback = 720

    # used for zscore calculation
    context.z_window = 360

    # Call strategy function after the London open every day
    schedule_function(pair_trading_strategy,
                     date_rules.every_day(),
                     time_rules.market_open(hours=9,minutes=30))

    # square off towards to NYC close
    context.trading_hours = False
    
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
    square_off(context)

def pair_trading_strategy(context,data):
    """
        function to define Pairs Trading strategy logic.
    """
    if context.trading_hours == False:
        return

    try:
        # Get the historic data for the stocks pair
        prices = data.history(  assets = [context.x, context.y],
                                fields = "close",
                                nbars = context.lookback,
                                frequency = "1m"
                             )
    except:
        return
    
    # drop nan values
    prices = prices.dropna()
    if len(prices) < 5:
        print(f'too few data points for z-score:{len(prices)}.')
        return
    
    # Take log of the prices
    prices = np.log(prices)

    # Store the price data in y and x
    y = prices[context.y]
    x = prices[context.x]

    # Calculate the hedge ratio and z_score
    _, context.hedge_ratio, resids = hedge_ratio(y, x)
    context.z_score = z_score(resids, lookback=context.z_window)
    # Compute the trading signal
    context.signal = trading_signal(context, data)

    # Place the order to trade the pair
    place_order(context)


def trading_signal(context, data):
    """
        determine the trade based on current z-score.
    """
    if context.z_score > context.entry_z_score:
        return -1
    elif context.z_score < -context.entry_z_score:
        return 1
    elif context.z_score < context.exit_z_score:
        return 0
    elif context.z_score > -context.exit_z_score:
        return 0
    return 999


def place_order(context):
    """
        A function to place order.
    """
    # no change in positioning
    if context.signal == 999:
        return

    weight = context.signal*context.leverage/2

    # cancel all outstanding orders
    cancel_all_open_orders(context)
    # send fresh orders
    order_target_percent(context.x, -weight*context.hedge_ratio)
    order_target_percent(context.y, weight)
