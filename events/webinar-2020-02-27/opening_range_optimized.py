"""
    Title: Opening Range breakout basic (NYSE)
    Description: This is a break-out strategy which goes long (short) on
        stocks that gap up (down) and close out same day or if the take 
        profit target is met.
    Style tags: Systematic
    Asset class: Equities, Futures, ETFs, Currencies and Commodities
    Dataset: NYSE Minute
"""
from blueshift_library.technicals.indicators import volatility
from blueshift_library.utils.utils import square_off
import numpy as np


from blueshift.finance import commission, slippage
from blueshift.api import(    symbol,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            set_commission,
                            set_slippage,
                       )


def daily_reset(context):
    """ reset the strategy variables. """
    # variable placeholders
    context.opening_ranges = {}     # store calculation of ranges
    context.signals = {}            # store signal function output
    context.weights = {}            # store position function output
    context.entry_levels = {}       # store entry prices
    context.volatilities = {}       # store historical 1 SD move
    context.size = {}               # store the signal strength
    context.state = None            # maintain state of the strategy

def create_universe(context):
    """ add list of stocks to create trading universe. """
    stocks = ['MSFT','AAPL','AMZN','FB','GOOG','BRK.B','JPM','JNJ',
                'XOM','V','BAC','INTC','PG','CSCO','DIS','HD','VZ',
                'CVX','MA']

    context.universe = [symbol(stock) for stock in stocks]

def calculate_trading_metrics(context, data):
    """ calculate opening range and entry levels """
    prices = data.history(context.universe, ['open','high','low','close'], 
                            context.lookback_data, '1d')
    for stock in context.universe:
        px = prices.xs(stock)
        px = px.dropna()
        current = px.iloc[-1]
        last = px.iloc[-2]
        px = px[:-1]
        vol = volatility(px.close.values, context.lookback_long)*15.874*last.close/100
        
        gap_up = current.low-last.high
        gap_down = last.low-current.high
        long_cond = gap_up/vol > 0. and gap_up/vol < 2.0
        short_cond = gap_down/vol > 0.0 and gap_down/vol < 2.0
        context.volatilities[stock] = vol
        
        # store the opening range for today
        if long_cond:
            context.opening_ranges[stock] = current.high, current.low, 'bullish', gap_up
        elif short_cond:
            context.opening_ranges[stock] = current.high, current.low, 'bearish', gap_down
        else:
            context.opening_ranges[stock] = None, None, 'neutral', None

    # set algo state for trading
    context.state = 'entry'

def no_more_entry(context, data):
    """ turn of further entry trades. """
    context.state = 'exit'

def unwind(context, data):
    """ turn trading off. """
    context.state = None
    square_off(context)
    daily_reset(context)

def handle_entry(context,data):
    """ apply the signal and position functions. """
    prices = data.current(context.universe, 'close')
    
    # apply the signal function
    for stock in context.universe:
        # if we already have position, ignore
        if stock in context.entry_levels:
            continue
        
        # just in case stock data is missing
        if stock not in context.opening_ranges:
            continue

        # get today's opening range
        high, low, mood, size = context.opening_ranges[stock]
        if prices[stock] > high and mood=='bullish':
            context.signals[stock] = 1
            context.size[stock] = size
        elif prices[stock] < low and mood=='bearish':
            context.signals[stock] = -1
            context.size[stock] = size

    # apply the position function
    if len(context.signals) == 0:
        # nothing to trade here
        return

    # else equal position in each of the stocks
    weight = context.leverage/len(context.signals)
    for stock in context.signals:
        # if we already have position, ignore
        if stock in context.entry_levels:
            continue
        x = context.size[stock]*context.signals[stock]
        pos = (-1 + 2/(1+np.exp(-5*x)))*weight
        #pos = (2 - 2/(1+np.exp(-x)))*weight
        #pos = pos*context.signals[stock]
        context.entry_levels[stock] = prices[stock]
        context.weights[stock] = pos
        order_target_percent(stock, context.weights[stock])

def handle_exit(context,data):
    """ exit if we hit our take profit target. """
    prices = data.current(context.universe, 'close')
    for stock in context.entry_levels:
        high, low, mood, size = context.opening_ranges[stock]
        current = prices[stock]
        entry = context.entry_levels[stock]
        vol = context.volatilities[stock]
        move = (current - entry)/vol
        if context.signals[stock] == 1 and move > context.profit_target:
            order_target_percent(stock, 0.0)
        elif context.signals[stock] == -1 and move < -context.profit_target:
            order_target_percent(stock, 0.0)

# use the platform API function for running our strategy

def initialize(context):
    """
        API function to define things to do at the start of the strategy.
    """
    # set strategy parameters
    context.lookback_data = 60
    context.lookback_long = 20
    context.leverage = 2.0
    context.profit_target = 1.0

    # reset everything at start
    daily_reset(context)

    # create our universe
    create_universe(context)

    # schedule calculation at the end of opening range (30 minutes)
    schedule_function(calculate_trading_metrics, date_rules.every_day(), 
                            time_rules.market_open(hours=0, minutes=30))
    
    # schedule entry rules
    schedule_function(no_more_entry, date_rules.every_day(), 
                            time_rules.market_open(hours=1, minutes=30))
    
    # schedule exit rules
    schedule_function(unwind, date_rules.every_day(), 
                            time_rules.market_close(hours=0, minutes=30))
    
    # set trading costs
    set_commission(commission.PerShare(cost=0.005, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))

def handle_data(context, data):
    """
        API function to define things to do at every bar.
    """
    if context.state is None:
        return
    elif context.state == 'entry':
        handle_entry(context, data)
    handle_exit(context, data)

