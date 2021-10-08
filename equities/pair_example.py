"""
    Title: Pairs Trading Startegy
    Description: This is a sample Pairs Trading strategy
    Style tags: Mean-reversion, Stat-Arb
    Asset class: Equities, Futures, ETFs, Currencies and Commodities
    Broker: NSE
"""
import numpy as np
from blueshift_library.utils.utils import z_score, hedge_ratio, cancel_all_open_orders


from blueshift.api import(    symbol,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                       )

def initialize(context):
    """
        function to define things to do at the start of the strategy
    """
    context.x = symbol('AMBUJACEM')
    context.y = symbol('ACC')
    context.leverage = 5
    context.signal = 0

    # Trade entry and exit when the z_score is +/- entry_z_score and exit_z_score respectively
    context.entry_z_score = 2.0
    context.exit_z_score = 0.5

    # Lookback window
    context.lookback = 200

    # used for zscore calculation
    context.z_window = 100

    # Call strategy function on the first trading day of each week at 10 AM
    schedule_function(pair_trading_strategy,
                     date_rules.week_start(),
                     time_rules.market_open(minutes=30))


def pair_trading_strategy(context,data):
    """
        function to define Pairs Trading strategy logic.
    """

    try:
        # Get the historic data for the stocks pair
        prices = data.history(  assets = [context.x, context.y],
                                fields = "close",
                                nbars = context.lookback,
                                frequency = "1d"
                             )
    except:
        return
    
    prices = prices.dropna()
    if len(prices) < 5:
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
