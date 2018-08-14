'''
    Title: FX Statistical (Lead-lag)
    Description: Statistical trading strategy: based on one period correlation
    Style tags: Risk Factor, Momentum, Stat-Arb
    Asset class: FX
    Dataset: FXCM Minute
'''
import pandas as pd
import numpy as np
import talib as ta

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
                      'rebalance_freq':'1h',
                      'no_overnight_position':True,
                      'pip_cost':0.00000,
                      'rollover_spread':0.00,     
                      'indicator_lookback':2000,    # max of all lookbacks!!!
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.45,
                      'sell_signal_threshold':-0.45}

    # define the strategy instruments universe
    context.universe = [
                               symbol('FXCM:AUD/USD'),
                               symbol('FXCM:EUR/USD'),
                               symbol('FXCM:GBP/USD'),
                               symbol('FXCM:NZD/USD'),
                               symbol('FXCM:USD/CAD'),
                               symbol('FXCM:USD/CHF'),
                             ]
    context.ccy_universe = ['AUD','CAD','CHF','EUR','GBP','JPY','NZD','USD']
    
    # function to schedule roll-overs, at 5 PM EST or 9 PM UTC (3 hours before midnight)
    schedule_function(compute_rollovers,
                    date_rules.every_day(),
                    time_rules.market_close(hours=2, minutes=45))
    
    # set up cost structures, we assume a $1 per $10K all-in cost
    set_commission(fx=commission.PipsCost(cost=context.params['pip_cost']))
    set_slippage(fx=slippage.FixedSlippage(spread=0.00))
    
    # variables to track signals and target portfolio
    context.signals = dict((security,0) for security in context.universe)
    context.weights = dict((security,0) for security in context.universe)
    context.correlations = pd.DataFrame({})

    # Call rebalance function, see below under standard helper functions to modify
    rebalance_scheduler(context)

    # try and make the run light-weight
    context.perf_tracker.todays_performance.keep_transactions = True
    context.perf_tracker.todays_performance.keep_orders = True

def before_trading_start(context, data):
    prices = data.history(context.universe, 'close', 
        context.params['indicator_lookback'], context.params['indicator_freq'])
    prices = prices.resample(str(context.trade_freq)+'T').last()
    prices = prices.diff().dropna()
    price_data = prices.iloc[:-1,]
    last_price_data = prices.iloc[1:,]

    correlations = {}
    same = {}
    opposite = {}
    # for c in price_data.columns:
    #     correlations[c]=price_data.corrwith(last_price_data.loc[:,c])
    
    # correlations = pd.DataFrame(correlations)
    correlations = prices.corr(method = 'spearman')
    correlations[correlations == 1] = 0

    for c in correlations.columns:
        same[c] = correlations[c].argmax()
        opposite[c] = correlations[c].argmin()

    context.correlations = correlations
    context.same = same
    context.opposite = opposite

def handle_data(context, data):
    # in case we are using scheduled function, we really don't use handle data
    if not context.use_handle_data:
        return
    
    # check if it is about time to trade
    context.bar_count = context.bar_count + 1
    if context.bar_count < context.trade_freq:
        return
    
    # reset count and run the strategy
    context.bar_count = 0
    rebalance(context, data)

def calculate_signal(context, price_data):
    '''
        The main trading logic goes here, called by generate_signals above
    '''
    if context.correlations.empty:
        return
    
    correlations = context.correlations
    price_data = price_data.diff().dropna()
    price_vol = price_data.std()
    price_move = price_data.iloc[-1]
    #context.signals = dict((security,0) for security in context.universe)
    for k in context.signals:
        context.signals[k] = 0

    for c in correlations.columns:
        same = context.same[c]
        opposite = context.opposite[c]
        move = 0
        
        if abs(price_move[c]/price_vol[c]) > 1.0:
            move = -np.sign(price_move[c])
            context.signals[c] = context.signals[c] + 1.0*move/len(context.universe)
            try:
                if correlations.loc[same,c] > 0.5:
                    context.signals[same] = context.signals[same] + 1.0*move/len(context.universe)
                if correlations.loc[opposite,c] < -0.5:
                    context.signals[opposite] = context.signals[opposite] - 1.0*move/len(context.universe)
            except:
                pass

def signal_function(context, data):
    num_secs = len(context.universe)
    weight = round(1.0/num_secs,2)*context.params['leverage']

    price_data = data.history(context.universe, 'close', 
        context.params['indicator_lookback'], context.params['indicator_freq'])
    price_data = price_data.resample(str(context.trade_freq)+'T').last()

    for c in context.signals:
        context.signals[c] = 0
    
    calculate_signal(context, price_data)
    
    context.last_price_data = price_data
    context.last_price_available = True

    for security in context.universe:
        if context.signals[security] == 999:
            pass # carry over last weight
        elif context.signals[security] > context.params['buy_signal_threshold']:
            context.weights[security] = weight
        elif context.signals[security] < context.params['sell_signal_threshold']:
            context.weights[security] = -weight
        else:
            pass

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
            print('trade freq {} minute(s)'.format(context.trade_freq))
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