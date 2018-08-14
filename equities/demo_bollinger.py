'''
    Title: Intraday Technical Strategies
    Description: This is a long short strategy based on bollinger bands and SMA dual signals
    Style tags: Systematic Fundamental
    Asset class: Equities, Futures, ETFs and Currencies
    Dataset: NSE Minute
'''
import talib as ta

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
'''
  A function to define things to do at the start of the strategy
'''
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

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
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
    price_data = data.history(context.securities, 'close', 
        context.params['indicator_lookback'], context.params['indicator_freq'])

    for security in context.securities:
        px = price_data.loc[:,security].values
        context.signals[security] = signal_function(px, context.params)

def signal_function(px, params):
    '''
        The main trading logic goes here, called by generate_signals above
    '''
    upper, mid, lower = bollinger_band(px,params['BBands_period'])
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
    sig = ta.CDLDOJI(px.open, px.high, px.low, px.close)
    return sig[-1]
