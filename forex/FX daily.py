
'''
    Title: FX Technicals (Bollinger Band)
    Description: FX strategy that runs a start of the day computations for many indicators 
        and can potentially use them during the day to entry and exit. Also controls the 
        trading time.
    Style tags: Risk Factor, Technicals, Mean Reversion
    Asset class: FX
    Dataset: FXCM Minute
'''
import pandas as pd
import numpy as np
import talib as ta
import bisect

# Zipline
from zipline.finance import commission, slippage
from zipline.api import(    symbol,
                            get_datetime,
                            order_target_percent,
                            order_target,
                            order_target_value,
                            schedule_function,
                            date_rules,
                            time_rules,
                            attach_pipeline,
                            pipeline_output,
                            set_commission,
                            set_slippage,
                            set_account_currency,
                            get_open_orders,
                            cancel_order
                       )
from zipline.datasets.macro import gdp, inflation, short_rates, long_rates

def initialize(context):
    '''
        Called once at the start of the strategy execution. 
        This is the place to define things to do at the start of the strategy.
    '''
    # set the account base currency and strategy parameters
    set_account_currency('USD')
    context.params = {'verbose':False,
                      'leverage':2,
                      'rebalance_freq':'15m',
                      'no_overnight_position':True,
                      'pip_cost':0.00005,
                      'rollover_spread':0.00,
                      'ADX_period':1440,         
                      'SMA_period_short':5,       
                      'SMA_period_long':20,     
                      'indicator_lookback':2400,    # max of all lookbacks!!!
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.5,
                      'sell_signal_threshold':-0.5,
                      'trading_time':[9,16],
                      'trading_stops':[1,1],
                      }

    # define the strategy instruments universe
    context.universe = [
                               symbol('FXCM:AUD/USD'),
                               symbol('FXCM:EUR/USD'),
                               symbol('FXCM:NZD/USD'),
                               symbol('FXCM:USD/CAD'),
                               symbol('FXCM:USD/CHF'),
                               symbol('FXCM:GBP/USD'),
                             ]
    context.ccy_universe = ['AUD','CAD','CHF','EUR','GBP','JPY','NZD','USD']
    
    # function to schedule roll-overs, at 5 PM EST or 9 PM UTC (3 hours before midnight)
    schedule_function(compute_rollovers,
                    date_rules.every_day(),
                    time_rules.market_close(hours=3, minutes=0))
    
    # set up cost structures, we assume a $1 per $10K all-in cost
    set_commission(fx=commission.PipsCost(cost=context.params['pip_cost']))
    set_slippage(fx=slippage.FixedSlippage(spread=0.00))
    
    context.pnls = []

    # Call rebalance function, see below under standard helper functions to modify
    rebalance_scheduler(context)

def before_trading_start(context, data):
    # check if positions were squared off
    if not get_positions(context).empty:
        raise ValueError('EOD square-off failed!')
    # variables to track signals and target portfolio
    context.signals = dict((security,0) for security in context.universe)
    context.weights = dict((security,0) for security in context.universe)
    context.indics = {}
    context.in_trade = dict((security,0) for security in context.universe)
    context.entry = dict((security,0) for security in context.universe)

    # compute pre-trade indicators
    px_data = data.history(context.universe,['open','high','low','close'],
                2880,'1m')
    px_minute = px_data.resample(str(context.trade_freq)+'T',axis=('major')).last()
    px_vols = px_minute['close'].diff().std()
    context.indics['vols'] = (10000*px_vols).round(1).to_dict()
    context.indics['range'] = (10000*(px_minute['close'].apply(price_range))).round().to_dict()
    context.indics['high'] = px_minute['close'].apply(max).to_dict()
    context.indics['low'] = px_minute['close'].apply(max).to_dict()
    context.indics['adx'] = px_minute.apply(adx, axis=('items','major')).to_dict()
    context.indics['rsi'] = px_minute.apply(rsi, axis=('items','major')).to_dict()
    context.indics['mom'] = px_minute.apply(mom, axis=('items','major')).to_dict()
    context.indics['DI+'] = px_minute.apply(plus_di, axis=('items','major')).to_dict()
    context.indics['DI-'] = px_minute.apply(minus_di, axis=('items','major')).to_dict()

def handle_data(context, data):
    # in case we are using scheduled function, we really don't use handle data
    if not context.use_handle_data:
        return
    
    # if we are not within trading time
    context.bar_count = context.bar_count + 1
    today = data.current_dt.date()
    elapsed = (data.current_dt.value - pd.Timestamp(today,tz='Etc/UTC').value)/1E9
    elapsed = elapsed/3600.
    if elapsed < context.params['trading_time'][0] or \
       elapsed > context.params['trading_time'][1]:
        return   
    
    # check if it is about time to trade
    if context.bar_count < context.trade_freq:
        return
    
    # reset count and run the strategy
    context.bar_count = 0
    rebalance(context, data)

def calculate_signal(px, params):
    '''
        The main trading logic goes here, called by generate_signals above
    '''
    lower, upper = fibonacci_support(px.close.values)
    ind2 = adx(px, params['ADX_period'])
    if lower == -1:
        return 1
    elif upper == -1:
        return -1
    elif upper > 0.02 and lower > 0 and upper/lower > 3 and ind2 < 20:
        return 1
    elif lower > 0.02 and upper > 0 and lower/upper > 3 and ind2 < 20:
        return -1
    else:
        return 999

def signal_function(context, data):
    num_secs = len(context.universe)
    weight = round(1.0/num_secs,2)*context.params['leverage']

    price_data = data.history(context.universe, ['open','high','low','close'], 
        context.params['indicator_lookback'], context.params['indicator_freq'])
    #price_data = price_data.resample(str(context.trade_freq)+'T').last()

    for security in context.universe:
        px = price_data.minor_xs(security)
        context.signals[security] = calculate_signal(px, context.params)
        in_trade = context.in_trade[security]
        vol = context.indics['vols'][security]
        adx_level = context.indics['adx'][security]
        rsi_level = context.indics['rsi'][security]
        plus_di_level = context.indics['DI+'][security]
        minus_di_level = context.indics['DI-'][security]
        target_level = context.params['trading_stops'][0]*vol
        stop_level = context.params['trading_stops'][1]*vol
        top_range = context.indics['high'][security] - vol
        bottom_range = context.indics['low'][security] + vol
        last_px = px['close'][-1]

        if context.signals[security] == 999:
            pass # carry over last weight
        elif context.signals[security] > context.params['buy_signal_threshold']:
            if context.in_trade[security] == 0 and vol > 5.0:
                context.weights[security] = weight
                context.in_trade[security] = 1
                context.entry[security] = last_px
                print('{} enter long trade in {}, vol {} adx {} rsi {}'.format(data.current_dt, security.symbol, vol, adx_level,top_range))
            elif context.in_trade[security] == -1:
                context.weights[security] = 0
                context.entry[security] = 0
                print('{} exit short trade in {} vol {} adx {} rsi {}'.format(data.current_dt, security.symbol,vol, adx_level,top_range))
        elif context.signals[security] < context.params['sell_signal_threshold']:
            if context.in_trade[security] == 0 and vol > 5.0:
                context.weights[security] = -weight
                context.in_trade[security] = -1
                context.entry[security] = last_px
                print('{} enter short trade in {} vol {} adx {} rsi {}'.format(data.current_dt, security.symbol,vol, adx_level,top_range))
            elif context.in_trade[security] == 1:
                context.in_trade[security] = 0
                context.entry[security] = 0
                print('{} exit long trade in {} vol {} adx {} rsi {}'.format(data.current_dt, security.symbol,vol, adx_level,top_range))
        elif last_px > (context.entry[security] + target_level) or \
                last_px < (context.entry[security] - stop_level):
            if context.in_trade[security] <> 0:
                context.weights[security] = 0
                context.entry[security] = 0     
                print('{} hit stop or target {} vol {} adx {}'.format(data.current_dt, security.symbol,vol, adx_level))


def rebalance(context,data):
    '''
        Rebalance positions of all instruments in the universe according to the computed
        weights. Expect context.weights, else rebalance to equally weighted portfolio
    '''
    signal_function(context, data)

    for security in context.weights:
        order_target_percent(security, context.weights[security])   

def analyze(context, performance):
    print(get_positions(context))
    print(context.portfolio.pnl)

################### standard helper functions #######################################
def rebalance_scheduler(context):
    '''
        function to schedule a rebalancing trade. 
        The rebalancing is done based on the weights determined in the strategy core
        for each of the instruments defined in the trading universe.
    '''
    context.use_handle_data = False
    rebalance_freq = context.params.get('rebalance_freq',None)

    if rebalance_freq is None:
        return
    
    if context.params['verbose']:
        print('setting up {} scheduler'.format(rebalance_freq))
    
    if rebalance_freq == 'monthly':
        schedule_function(rebalance,
                    date_rules.month_start(days_offset=0),
                    time_rules.market_open(hours=5, minutes=30))
    elif rebalance_freq == 'weekly':
        schedule_function(rebalance,
                    date_rules.week_start(days_offset=0),
                    time_rules.market_open(hours=5, minutes=30))
    elif rebalance_freq == 'daily':
        schedule_function(rebalance,
                    date_rules.every_day(),
                    time_rules.market_open(hours=5, minutes=30))
    elif rebalance_freq.endswith('m'):
        try:
            context.trade_freq = int(rebalance_freq.split('m')[0])
            print('trade freq {} minute(s)'.format(context.trade_freq))
            context.bar_count = 0
            context.use_handle_data = True
        except:
            raise ValueError('Invalid minute frequency')
    elif rebalance_freq.endswith('h'):
        try:
            context.trade_freq = int(rebalance_freq.split('h')[0])*60
            print('trade freq {} hours(s)'.format(context.trade_freq/60))
            context.bar_count = 0
            context.use_handle_data = True
        except:
            raise ValueError('Invalid hourly frequency')
    else:
        raise ValueError('Un-recognized rebalancing frequency')

    if context.params['no_overnight_position']:
        schedule_function(square_off,
                    date_rules.every_day(),
                    time_rules.market_close(hours=3, minutes=30))

def cancel_all_open_orders(context, data, asset=None):
    '''
        Cancel all open orders on a particular assets, or all if asset is None.
    '''
    if asset:
        open_orders = get_open_orders(asset)
    else:
        open_orders = get_open_orders()

    try:
        iter(open_orders)
    except:
        open_orders = [open_orders]
        
    if open_orders:
        for asset in open_orders:
            if context.params['verbose']:
                print('cancelling order on {}'.format(asset.symbol))
            cancel_order(asset)

def square_off(context, data, asset=None):
    '''
        Square off position in a particular asset, or all if asset is None
    '''
    cancel_all_open_orders(context, data, asset)
    
    if asset:
        positions_to_unwind = [context.portfolio.positions[asset]]
    else:
        positions_to_unwind = context.portfolio.positions

    for asset in context.portfolio.positions:
        if context.portfolio.positions[asset]['amount'] <> 0:
            if context.params['verbose']:
                print('squaring of {}'.format(asset.symbol))
            order_target_percent(asset, 0.0)
    pass

def compute_rollovers(context, data):
    next_open = context.trading_calendar.next_open(data.current_dt)
    days = (next_open.date() - data.current_dt.date()).days
    if days > 1:
        days = days + 1
    
    positions = get_positions(context)
    rates_3m = short_rates.current(context,context.ccy_universe)
    carry = 0
    
    for asset, row in positions.iterrows():
        rates_differential = (rates_3m[ asset.base_ccy] - rates_3m[asset.quote_ccy])/100.0
        rates_spread = 2*context.params['rollover_spread']/100.0
        carry_effective_notional = row['amount']*row['last_sale_price']*row['last_fx_value']
        carry_cost = carry_effective_notional*rates_differential
        carry_spread_cost = abs(carry_effective_notional)*rates_spread
        carry = carry + carry_cost - carry_spread_cost
    
    # we assume ACT/360 convention for all currencies
    daily_carry = (carry * days)/ 360.0
    context.perf_tracker.cumulative_performance.handle_cash_payment(daily_carry)
    context.perf_tracker.todays_performance.handle_cash_payment(daily_carry)

def get_positions(context):
    '''
        Get a list of current positions as a Pandas Dataframe
    '''
    positions = {}
    for asset in context.portfolio.positions:
        pos = context.portfolio.positions[asset]
        d = {'amount':pos.amount,'cost_basis':pos.cost_basis,
         'last_sale_price':pos.last_sale_price,'last_fx_value':pos.last_fx_value}
        positions[asset] = d
    
    return pd.DataFrame(positions).transpose()

def get_portfolio_details(context):
    p = {'value':context.portfolio.portfolio_value,
         'pnl':context.portfolio.pnl,
         'cash':context.portfolio.cash}
    return p
    
################### standard technical indicator functions #############################
def sma(px, lookback):
    try:
        px = px.close.values
    except:
        pass
    sig = ta.SMA(px, timeperiod=lookback)
    return sig[-1]

def ema(px, lookback):
    try:
        px = px.close.values
    except:
        pass
    sig = ta.EMA(px, timeperiod=lookback)
    return sig[-1]

def rsi(px, lookback=28):
    try:
        px = px.close.values
    except:
        pass
    sig = ta.RSI(px, timeperiod=lookback)
    return sig[-1]

def bollinger_band(px, lookback):
    try:
        px = px.close.values
    except:
        pass
    upper, mid, lower = ta.BBANDS(px, timeperiod=lookback)
    return upper[-1], mid[-1], lower[-1]

def macd(px, lookback):
    try:
        px = px.close.values
    except:
        pass
    macd_val, macdsignal, macdhist = ta.MACD(px)
    return macd_val[-1], macdsignal[-1], macdhist[-1]

def doji(px):
    sig = ta.CDLDOJI(px.open.values, px.high.values, px.low.values, px.close.values)
    return sig[-1]

def price_range(px):
    try:
        px = px.close.values
    except:
        pass
    return max(px) - min(px)

def adx(px, lookback=28):
    sig = ta.ADX(px.high.values, px.low.values, px.close.values, timeperiod=lookback)
    return round(sig[-1],4)

def minus_di(px, lookback):
    sig = ta.MINUS_DI(px.high.values, px.low.values, px.close.values, lookback)
    return round(sig[-1],4)

def plus_di(px, lookback):
    sig = ta.PLUS_DI(px.high.values, px.low.values, px.close.values, lookback)
    return round(sig[-1],4)

def mom(px, lookback=28):
    try:
        px = px.close.values
    except:
        pass
    sig = ta.MOM(px, timeperiod=lookback)
    return round(sig[-1],4)

def minus_di(px, lookback=28):
    sig = ta.MINUS_DI(px.high.values, px.low.values, px.close.values, lookback)
    return round(sig[-1],4)

def plus_di(px, lookback=28):
    sig = ta.PLUS_DI(px.high.values, px.low.values, px.close.values, lookback)
    return round(sig[-1],4)

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
