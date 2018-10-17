'''
    Title: Classic (Jegadeesh and Titman) cross-sectional momentum (equal weights)
    Description: This strategy uses past returns to rank securities
                and go long (short) the top (bottom) n-percentile
    Style tags: Momentum
    Asset class: Equities, Futures, ETFs, Currencies
    Dataset: All
'''
import numpy as np

# Zipline
from zipline.pipeline import Pipeline, CustomFilter, CustomFactor
from zipline.pipeline.data import EquityPricing
from zipline.errors import NoFurtherDataError
from zipline.api import(    symbol,
                            sid,
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
    # The context variables can be accessed by other methods
    context.params = {'lookback':12,
                      'holding':1,
                      'percentile':0.05,
                      'min_volume':1E7
                      }

    # rebalance tracker: required for hp > 1 month
    context.rebalance_count = 0
    
    # Call rebalance function on the first trading day of each month at 12 noon
    schedule_function(strategy, date_rules.month_start(), 
            time_rules.market_close(minutes=1))

    # Set up the pipe-lines for strategies
    attach_pipeline(make_strategy_pipeline(context), 
            name='strategy_pipeline')

def strategy(context, data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        return
    
    p = context.params['percentile']
    momentum = pipeline_results.dropna().sort_values('momentum')
    n = int(len(momentum)*p)
    context.long_securities = momentum.index[-n:]
    context.short_securities = momentum.index[:n]
    print("*"*35)
    print([s.symbol for s in context.long_securities])

    # weighing function
    weight = 0.5/n

    # square off old positions if required
    for security in context.portfolio.positions:
        if security not in context.long_securities and \
           security not in context.short_securities:
               order_target_percent(security, 0)

    # Place orders for the new portfolio
    for security in context.long_securities:
        order_target_percent(security, weight)
    for security in context.short_securities:
        order_target_percent(security, -weight)


############################ pipelines #############################
def average_volume_filter(lookback, amount):
    class AvgDailyDollarVolumeTraded(CustomFilter):
        inputs = [EquityPricing.close, EquityPricing.volume]
        def compute(self,today,assets,out,close_price,volume):
            dollar_volume = np.mean(close_price * volume, axis=0)
            high_volume = dollar_volume > amount
            out[:] = high_volume
    return AvgDailyDollarVolumeTraded(window_length = lookback)

def period_returns(lookback, volume_filter):
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            start_price = close_price[0]
            end_price = close_price[-1]
            returns = end_price/start_price - 1
            out[:] = returns
    return SignalPeriodReturns(window_length = lookback, mask=volume_filter)

def make_strategy_pipeline(context):
    pipe = Pipeline()

    # get the strategy parameters
    lookback = context.params['lookback']*21
    v = context.params['min_volume']

    # Set the volume filter
    volume_filter = average_volume_filter(lookback, v)
    
    # compute past returns
    momentum = period_returns(lookback,volume_filter)
    pipe.add(momentum,'momentum')

    return pipe
