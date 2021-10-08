"""
    Title: Classic (Pedersen) time-series momentum (equal weights)
    Description: This strategy uses past returns and go long (short) 
                the positive (negative) n-percentile
    Style tags: Momentum
    Asset class: Equities, Futures, ETFs, Currencies
    Broker: NSE/ US Equities
"""
from blueshift_library.pipelines.pipelines import average_volume_filter, period_returns

from blueshift.pipeline import Pipeline
from blueshift.errors import NoFurtherDataError
from blueshift.pipeline.data import EquityPricing
from blueshift.api import(
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            attach_pipeline,
                            pipeline_output,
                            get_datetime
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
    momentum = period_returns(lookback)
    pipe.add(momentum,'momentum')
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
    momentum = pipeline_results
    
    long_candidates = momentum[momentum > 0].dropna().sort_values('momentum')
    short_candidates = momentum[momentum < 0].dropna().sort_values('momentum')
    
    n_long = len(long_candidates)
    n_short = len(short_candidates)
    n = int(min(n_long,n_short)*p)

    if n == 0:
        print("{}, no signals".format(get_datetime()))
        context.long_securities = []
        context.short_securities = []
    
    context.long_securities = long_candidates.index[-n:]
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
