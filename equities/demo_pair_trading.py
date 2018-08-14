'''
    Title: Intraday Technical Strategies
    Description: This is a pair trading strategy using linear regression and 
        stationarity test
    Style tags: momentum and mean reversion
    Asset class: Equities, Futures, ETFs and Currencies
    Dataset: NSE Minute
'''
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller

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
    context.params = {'indicator_lookback':1875,
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.5,
                      'sell_signal_threshold':-0.5,
                      'lookback':1175,
                      'p_threshold':0.05,
                      'zscore':1.1,
                      'trade_freq':5,
                      'leverage':10}
    
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
    for security in context.securities:
        context.target_position[security] = context.signals[security]*context.params['leverage']
    
def generate_signals(context, data):
    '''
        A function to define define the signal generation
    '''
    price_data = data.history(context.securities, ['open','high','low','close'], 
        context.params['indicator_lookback'], context.params['indicator_freq'])
    
    px1 = price_data.minor_xs(context.securities[0])
    px2 = price_data.minor_xs(context.securities[1])
    last_signals = (context.signals[context.securities[0]], context.signals[context.securities[1]])
    signals = signal_function(px1, px2, context.params, last_signals)
    context.signals[context.securities[0]] = signals[0]
    context.signals[context.securities[1]] = signals[1]

def signal_function(px1, px2, params, last_signal):
    '''
        The main trading logic goes here, called by generate_signals above
    '''
    # we have tighter bound on p-value and more relaxed on z-score
    p_value, zscore, hedge_ratio = pair_stats(px1.close.values, px2.close.values, 
        params['lookback'], params['p_threshold'])
    
    if zscore == -1:
        return 0, 0
    elif zscore > params['zscore']:
        return -1, hedge_ratio
    elif zscore < -params['zscore']:
        return 1, -hedge_ratio
    else:
        return last_signal

def pair_stats(px1, px2, lookback, p_threshold):
    p_value = zscore = hedge_ratio = -1

    px1 = px1[-lookback:-1]
    px2 = px2[-lookback:-1]

    # fit a linear model and test the residuals for stationarity
    model = sm.OLS(px1, px2).fit()
    resids = model.resid
    p_value = adfuller(resids)[1]

    if p_value < p_threshold:
        zscore = round(compute_zscore(resids),2)
        hedge_ratio = round(model.params[0],2)
    
    return p_value, zscore, hedge_ratio

def compute_zscore(x):
    sd = np.std(x)
    mean = np.mean(x)
    zscore = (x[-1] - mean)/sd
    return zscore

