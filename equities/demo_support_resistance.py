
'''
    Title: Intraday Technical Strategies
    Description: This is a long short strategy based on Fibonacci support and resistance.
        Goes with the momentum for levels break-outs, else buys near support and sells
        near resistance if confirmed by ADX
    Style tags: momentum and mean reversion
    Asset class: Equities, Futures, ETFs and Currencies
    Dataset: NSE Daily or NSE Minute
'''
import talib as ta
import numpy as np
import bisect

# Zipline
from zipline.finance import commission, slippage
from zipline.api import(    symbol,
                            get_datetime,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            attach_pipeline,
                            pipeline_output,
                            set_commission,
                            set_slippage,
                            get_open_orders,
                            cancel_order
                       )

def initialize(context):
    '''
        A function to define things to do at the start of the strategy
    '''
    # universe selection
    context.securities = [symbol('NIFTY-I'),symbol('BANKNIFTY-I')]
    
    # define strategy parameters
    context.params = {'indicator_lookback':375,
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.5,
                      'sell_signal_threshold':-0.5,
                      'ROC_period_short':30,
                      'ROC_period_long':120,
                      'ADX_period':120,
                      'trade_freq':5,
                      'leverage':2}
    
    # variable to control trading frequency
    context.bar_count = 0

    # variables to track signals and target portfolio
    context.signals = dict((security,0) for security in context.securities)
    context.target_position = dict((security,0) for security in context.securities)

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.002, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))


def handle_data(context, data):
    '''
        A function to define things to do at every bar
    '''
    context.bar_count = context.bar_count + 1
    if context.bar_count < context.params['trade_freq']:
        return
    
    # time to trade, call the strategy function
    context.bar_count = 0
    run_strategy(context, data)
    

def run_strategy(context, data):
    '''
        A function to define core strategy steps
    '''
    generate_signals(context, data)
    generate_target_position(context, data)
    rebalance(context, data)

def rebalance(context,data):
    '''
        A function to rebalance - all execution logic goes here
    '''
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
    '''
        A function to define define the signal generation
    '''
    price_data = data.history(context.securities, ['open','high','low','close'], 
        context.params['indicator_lookback'], context.params['indicator_freq'])

    for security in context.securities:
        px = price_data.minor_xs(security)
        context.signals[security] = signal_function(px, context.params,
            context.signals[security])

def signal_function(px, params, last_signal):
    '''
        The main trading logic goes here, called by generate_signals above
    '''
    lower, upper = fibonacci_support(px.close.values)
    ind2 = adx(px, params['ADX_period'])

    if lower == -1:
        return -1
    elif upper == -1:
        return 1
    elif upper > 0.02 and lower > 0 and upper/lower > 3 and ind2 < 20:
        return -1
    elif lower > 0.02 and upper > 0 and lower/upper > 3 and ind2 < 20:
        return 1
    else:
        return last_signal

def sma(px, lookback):
    sig = ta.SMA(px, timeperiod=lookback)
    return sig[-1]

def ema(px, lookback):
    sig = ta.EMA(px, timeperiod=lookback)
    return sig[-1]

def rsi(px, lookback):
    sig = ta.RSI(px, timeperiod=lookback)
    return sig[-1]

def bollinger_band(px, lookback):
    upper, mid, lower = ta.BBANDS(px, timeperiod=lookback)
    return upper[-1], mid[-1], lower[-1]

def macd(px, lookback):
    macd_val, macdsignal, macdhist = ta.MACD(px)
    return macd_val[-1], macdsignal[-1], macdhist[-1]

def doji(px):
    sig = ta.CDLDOJI(px.open.values, px.high.values, px.low.values, px.close.values)
    return sig[-1]

def roc(px, lookback):
    signal = ta.ROC(px, timeperiod=lookback)
    return signal[-1]

def adx(px, lookback):
    signal = ta.ADX(px.high.values, px.low.values, px.close.values, timeperiod=lookback)
    return signal[-1]

def fibonacci_support(px):
    def fibonacci_levels(px):
        return [min(px) + l*(max(px) - min(px)) for l in [0,0.236,0.382,0.5,0.618,1]]

    def find_interval(x, val):
        return (-1 if val < x[0] else 99) if val < x[0] or val > x[-1] \
            else  max(bisect.bisect_left(x,val)-1,0)

    last_price = px[-1]
    lower_dist = upper_dist = 0
    sups = fibonacci_levels(px[:-1])
    idx = find_interval(sups, last_price)

    if idx==-1:
        lower_dist = -1
        upper_dist = round(100.0*(sups[0]/last_price-1),2)
    elif idx==99:
        lower_dist = round(100.0*(last_price/sups[-1]-1),2)
        upper_dist = -1
    else:
        lower_dist = round(100.0*(last_price/sups[idx]-1),2)
        upper_dist = round(100.0*(sups[idx+1]/last_price-1),2)

    return lower_dist,upper_dist
