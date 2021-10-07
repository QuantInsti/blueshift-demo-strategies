"""
    Title: ambiguity loving factor
    Description: This strategy uses volatility to seggregate portfolio, 
        then sort the portfolios by skew and go long low vol positive 
        skew stocks vs high vol positive skew ones. See 
        https://www.tandfonline.com/doi/full/10.1080/23322039.2019.1693678
    Style tags: volatility factor
    Asset class: Equities, Futures, ETFs, Currencies
    Dataset: All
"""
from blueshift_library.pipelines.pipelines import average_volume_filter, technical_factor
from blueshift_library.technicals.indicators import volatility
from scipy.stats import skew

from blueshift.pipeline import Pipeline
from blueshift.errors import NoFurtherDataError
from blueshift.api import(
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            attach_pipeline,
                            pipeline_output,
                            get_datetime,
                       )

def initialize(context):
    '''
        A function to define things to do at the start of the strategy
    '''
    # The context variables can be accessed by other methods
    context.params = {'lookback':12,
                      'percentile':0.1,
                      'min_volume':1E8
                      }
    
    # Call rebalance function on the first trading day of each month
    schedule_function(strategy, date_rules.month_start(), 
            time_rules.market_close(minutes=1))

    # Set up the pipe-lines for strategies
    attach_pipeline(make_strategy_pipeline(context), 
            name='strategy_pipeline')

def strategy(context, data):
    generate_signals(context, data)
    rebalance(context,data)

def make_strategy_pipeline(context):
    pipe = Pipeline()

    # get the strategy parameters
    lookback = context.params['lookback']*21
    v = context.params['min_volume']

    # Set the volume filter
    volume_filter = average_volume_filter(lookback, v)
    
    # compute past returns
    vol_factor = technical_factor(lookback, volatility, 1)
    skew_factor = technical_factor(lookback, skewness, None)
    pipe.add(vol_factor,'vol')
    pipe.add(skew_factor,'skew')
    pipe.set_screen(volume_filter)

    return pipe

def generate_signals(context, data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        context.long_securities = []
        context.short_securities = []
        return
    
    p = context.params['percentile']
    pipeline_results = pipeline_results[pipeline_results.vol > 0].dropna()
    vol_threshold_hi = pipeline_results.vol.quantile(0.75)
    vol_threshold_lo = pipeline_results.vol.quantile(0.25)
    high_vol = pipeline_results[pipeline_results.vol > vol_threshold_hi]
    low_vol = pipeline_results[pipeline_results.vol < vol_threshold_lo]
    
    short_candidates = high_vol.sort_values('skew',ascending=False)
    long_candidates = low_vol.sort_values('skew',ascending=False)
    available = min(len(long_candidates),len(short_candidates))
    
    n = int(available*p)
    print('total candidates {}'.format(n))
    
    if n == 0:
        print("{}, no signals".format(get_datetime()))
        context.long_securities = []
        context.short_securities = []

    context.long_securities = long_candidates.index[:n]
    context.short_securities = short_candidates.index[:n]

def rebalance(context,data):
    # weighing function
    n = len(context.long_securities)
    if n < 1:
        return
        
    weight = 0.5/n

    # square off old positions if any
    for security in context.portfolio.positions:
        if security not in context.long_securities and \
           security not in context.short_securities:
               order_target_percent(security, 0)

    # Place orders for the new portfolio
    for security in context.long_securities:
        order_target_percent(security, weight)
    for security in context.short_securities:
        order_target_percent(security, -weight)

def skewness(px, lookback=None):
    return skew(px)
