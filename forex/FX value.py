'''
    Title: FX value
    Description: Value strategy for FX. Among G10, pick top 2 to go long and bottom 2 
        to go short, based on PPP valuation. Re-balance every month
    Style tags: Risk Factor, Value
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
                      'leverage':1,
                      'rebalance_freq':'monthly',
                      'no_overnight_position':False,
                      'pip_cost':0.0001,
                      'rollover_spread':0.00,
                      'positions':2}

    # define the strategy instruments universe
    context.universe = [
                               symbol('FXCM:AUD/USD'),
                               symbol('FXCM:EUR/CHF'),
                               symbol('FXCM:EUR/JPY'),
                               symbol('FXCM:EUR/USD'),
                               symbol('FXCM:GBP/JPY'),
                               symbol('FXCM:GBP/USD'),
                               symbol('FXCM:NZD/USD'),
                               symbol('FXCM:USD/CAD'),
                               symbol('FXCM:USD/CHF'),
                               symbol('FXCM:USD/JPY'),
                             ]
    context.ccy_universe = ['AUD','CAD','CHF','EUR','GBP','JPY','NZD','USD']
    
    # function to schedule roll-overs, at 5 PM EST or 9 PM UTC (3 hours before midnight)
    schedule_function(compute_rollovers,
                    date_rules.every_day(),
                    time_rules.market_close(hours=3, minutes=0))
    
    # set up cost structures, we assume a $1 per $10K all-in cost
    set_commission(fx=commission.PipsCost(cost=context.params['pip_cost']))
    set_slippage(fx=slippage.FixedSlippage(spread=0.00))
    
    # Call rebalance function, see below under standard helper functions to modify
    rebalance_scheduler(context)

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

def signal_function_valuation(context, data):
    inf = inflation.current(context,context.ccy_universe)
    fx_data = data.history(context.universe,'close',250,'1d')
    valuations = {}

    for pair in context.universe:
        inf_ratio = (1 + inf[pair.base_ccy]/100.0)/(1 + inf[pair.quote_ccy]/100.0)
        fx_ratio = fx_data[pair][-1]/fx_data[pair][0]
        valuations[pair] = inf_ratio - fx_ratio
        print('{}:pair {}, inf ratio {}, currency ratio {}, val {}'.format(data.current_dt,
                pair,inf_ratio, fx_ratio, valuations[pair]))
    
    sorted_universe = sorted(valuations.items(), key=lambda x: x[1])
    n_pos = context.params['positions']
    short_pos = [(k[0],-0.5/n_pos) for k in sorted_universe[:n_pos]]
    zero_pos = [(k[0],0.0) for k in sorted_universe[n_pos:-n_pos]]
    long_pos = [(k[0],0.5/n_pos) for k in sorted_universe[-n_pos:]]

    context.weights = dict(short_pos+zero_pos+long_pos)

def rebalance(context,data):
    '''
        Rebalance positions of all instruments in the universe according to the computed
        weights. Expect context.weights, else rebalance to equally weighted portfolio
    '''
    signal_function_valuation(context, data)
    
    for security in context.weights:
        leverage = context.params['leverage']
        order_target_percent(security, leverage*context.weights[security])

def analyze(context, performance):
    print(get_positions(context))


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
            print('trade freq {}'.format(context.trade_freq))
            context.bar_count = 0
            context.use_handle_data = True
        except:
            raise ValueError('Invalid minute frequency')
    elif rebalance_freq.endswith('h'):
        try:
            context.trade_freq = int(rebalance_freq.split('h')[0])*60
            print('trade freq {}'.format(context.trade_freq))
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

    for asset in positions_to_unwind:
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
        rates_differential = (rates_3m[asset.quote_ccy]-rates_3m[ asset.base_ccy])/100.0
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

