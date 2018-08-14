'''
    Title: Intraday Technical Strategies
    Description: This is a long short strategy based on short term reversal. We essentially
        estimate the underlying trends with Random Forest and take an opposite position
        expecting a short term reversal
    Style tags: Systematic Fundamental
    Asset class: Equities, Futures, ETFs and Currencies
    Dataset: NSE Daily or NSE Minute
'''
import talib as ta
import numpy as np
from sklearn.ensemble import RandomForestRegressor

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
    context.params = {'indicator_lookback':3750,
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.5,
                      'sell_signal_threshold':-0.5,
                      'lookback_period':375,
                      'lookback_period_short':60,
                      'trade_freq':30,
                      'model_estimate_freq':2,
                      'leverage':2}
    
    # variable to control trading frequency
    context.bar_count = 0
    
    # variable to control model estimate frequency
    context.day_count = 0

    # variables to track signals and target portfolio
    context.models = dict((security,0) for security in context.securities)
    context.signals = dict((security,0) for security in context.securities)
    context.target_position = dict((security,0) for security in context.securities)

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))

def before_trading_start(context, data):
    '''
        A function to define things to do at the start of each trading day
    '''
    if context.day_count % context.params['model_estimate_freq'] != 0:
        context.day_count = context.day_count + 1
        return
    
    # time to re-evaluate our model
    context.day_count = context.day_count + 1
    price_data = data.history(context.securities, ['open','high','low','close','volume'], 
        context.params['indicator_lookback'], context.params['indicator_freq'])

    for security in context.securities:
        px = price_data.minor_xs(security)
        context.models[security] = model_estimator(px, context.params,
            context.models[security])

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
    price_data = data.history(context.securities, ['open','high','low','close','volume'], 
        context.params['indicator_lookback'], context.params['indicator_freq'])

    for security in context.securities:
        px = price_data.minor_xs(security)
        context.signals[security] = signal_function(px, context.params, 
            context.models[security], context.signals[security])

def signal_function(px, params, model, old_signal):
    '''
        The main trading logic goes here, called by generate_signals above
    '''
    df = get_signals_df(px, params)
    try:
        pred = predict_random_forest(model, df)
        signal = -1 if pred > 0.05 else (1 if pred < -0.05 else 0)
    except:
        print('signal generation failed, carrying over past signal')
        signal = old_signal
    
    return signal

def model_estimator(px, params, old_model):
    '''
        The model estimator function
    '''
    df = get_training_data(px, params)
    try:
        regr = estimate_random_forest(df)
    except:
        regr = old_model
    
    return regr

def get_signals_df(px, params):
    '''
        This functions generates a large number of arbitrarily selected technical signals
        with the intention to throw them to a random forest regressor to capture the 
        underlying momentum. See http://prodiptag.blogspot.com/2016/08/systematic-trading-getting-technical.html
        for a background
    '''
    target = 100*(px.close.shift(-5)/px.close - 1)
    upperband, middleband, lowerband = ta.BBANDS(px.close.values, params['lookback_period'])
    bollinger = 100*(upperband - px.close.values)/(upperband - lowerband)
    xma = ta.EMA(px.close.values, params['lookback_period']) \
        - ta.EMA(px.close.values, params['lookback_period_short'])
    adx = ta.ADX(px.high.values, px.low.values, px.close.values, params['lookback_period'])
    aroon = ta.AROONOSC(px.high.values, px.low.values, params['lookback_period'])
    cci = ta.CCI(px.high.values, px.low.values, px.close.values, params['lookback_period'])
    cmo = ta.CMO(px.close.values, params['lookback_period'])
    _, _, macd = ta.MACD(px.close.values, fastperiod=params['lookback_period']/2, 
        slowperiod=params['lookback_period'], signalperiod=9)
    rsi = ta.RSI(px.close.values, params['lookback_period'])
    williamsR = ta.WILLR(px.high.values, px.low.values, px.close.values, 
        params['lookback_period'])
    chaikin = ta.ADOSC(px.high.values, px.low.values, px.close.values, px.volume.values, 
        params['lookback_period_short'], params['lookback_period'])
    atr = ta.ATR(px.high.values, px.low.values, px.close.values, params['lookback_period'])
    doji =  ta.CDLDOJI(px.open.values, px.high.values, px.low.values, px.close.values)

    df = np.column_stack((bollinger,xma,adx,aroon,cci,cmo,macd,rsi,williamsR,chaikin, 
        atr, doji, target.values))
    return df

def get_training_data(px, params):
    df = get_signals_df(px, params)
    mask = np.any(np.isnan(df), axis=1)
    df = df[~mask]
    return df

def estimate_random_forest(df):
    regr = RandomForestRegressor()
    regr.fit(df[:,:-1], df[:,-1:].ravel())
    return regr
    
def predict_random_forest(regr, df):
    pred = regr.predict(df[-1:,:-1])[0]
    return pred
    
