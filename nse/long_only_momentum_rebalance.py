"""
    Title: Long-only momentum portfolio rebalancer
    Description: This strategy uses past returns to rank securities
                and go long the top (bottom) n assets.
    Style tags: Momentum
    Asset class: Equities, ETFs
    Dataset: NSE
    Risk: Medium
    Minimum Capital: 10,000
"""
import datetime

from blueshift.library.pipelines import period_returns
from blueshift.pipeline.factors import AverageDollarVolume
from blueshift.library.pipelines import filter_universe

from blueshift.pipeline import Pipeline
from blueshift.errors import NoFurtherDataError
from blueshift.api import(
                            symbol,
                            order_target_value,
                            schedule_function,
                            date_rules,
                            time_rules,
                            attach_pipeline,
                            pipeline_output,
                            get_datetime,
                            set_long_only,
                            set_algo_parameters,
                            terminate,
                       )

def next_month(dt):
    if dt.month == 12:
        new = datetime.datetime(year=dt.year+1,month=1, day=1)
    else:
        new = datetime.datetime(year=dt.year,month=dt.month+1, day=1)
        
    return new.strftime('%B'), new.year
        

def initialize(context):
    context.strategy_name = 'Long-only momentum portfolio rebalancer'
    context.weights = {}
    context.dynamic = False
    
    # The context variables can be accessed by other methods
    context.params = {'lookback':12,        # lookback for momentum
                      'num_stocks':10,      # max assets to trade
                      'universe':100,       # filtering on liquidity
                      'order_value':None    # for each stock
                      }
    
    set_algo_parameters('params') # the attribute of context
    print(f'got params {context.params}')
    
    try:
        context.params['lookback'] = int(context.params['lookback'])
        assert context.params['lookback'] <= 12
        assert context.params['lookback'] >= 3
    except:
        msg = 'lookback must be an integer between 3 and 12 (months).'
        raise ValueError(msg)
        
    try:
        context.params['num_stocks'] = int(context.params['num_stocks'])
        assert context.params['num_stocks'] <= 20
        assert context.params['num_stocks'] >= 2
    except:
        msg = 'num_stocks must be an integer between 2 and 20.'
        raise ValueError(msg)
        
    try:
        context.params['universe'] = int(context.params['universe'])
        assert context.params['universe'] <= 500
        assert context.params['universe'] >= 50
        context.dynamic = True
    except:
        try:
            universe = context.params['universe'].split(',')
            universe = [symbol(s.strip()) for s in universe]
            context.weights = {asset:1 for asset in universe}
        except:
            msg = 'universe must be an integer between 50 and 500 or '
            msg += 'a list of stocks (comma separated).'
            raise ValueError(msg)
        
    if not context.params['order_value']:
        capital = context.portfolio.starting_cash
        context.params['order_value'] = capital/context.params['num_stocks']
        try:
            assert context.params['order_value'] <= 50000
            assert context.params['order_value'] >= 1000
        except:
            msg = f'order_value is {context.params["order_value"]} based on '
            msg += f'initial capital {context.portfolio.starting_cash} '
            msg += f'and number of stocks {context.params["num_stocks"]}'
            msg += f', but it must be between 500 and 50,000. Please '
            msg += 'adjust initial capital or number of stocks.'
            raise ValueError(msg)
    else:
        try:
            context.params['order_value'] = float(context.params['order_value'])
            assert context.params['order_value'] <= 50000
            assert context.params['order_value'] >= 1000
        except:
            msg = 'order_value must be a number between 500 and 50,000.'
            raise ValueError(msg)
            
    required = context.params['order_value']*context.params['num_stocks']
    capital = context.portfolio.starting_cash
    if capital < required:
        msg = f'Required capital is {required}, alloted {capital}, '
        msg += f'please add more capital and try again.'
        raise ValueError(msg)
        
    # set long only
    set_long_only()
    
    # set weights in case of static universe
    if context.weights:
        value = context.params['order_value']
        for asset in context.weights:
            context.weights[asset] = value
    
    # Call rebalance function on the first trading day of each month
    schedule_function(strategy, date_rules.month_start(), 
            time_rules.market_open(minutes=45))

    # Set up the pipe-lines for strategies
    attach_pipeline(make_strategy_pipeline(context), 
            name='strategy_pipeline')
    
    msg = f'Starting strategy {context.strategy_name} '
    msg += f'with parameters {context.params}'
    print(msg)
    context.traded = False
    context.mock = True
    
def handle_data(context, data):
    if context.mock and not context.traded:
        print(f'Initialting portfolio rebalance once.')
        strategy(context, data)
        context.traded = True

def strategy(context, data):
    generate_signals(context, data)
    rebalance(context,data)

def make_strategy_pipeline(context):
    pipe = Pipeline()

    lookback = context.params['lookback']*21
    
    if context.dynamic:
        top_n = context.params['universe']
        screener = AverageDollarVolume(
                window_length=lookback).top(top_n)
    else:
        universe = list(context.weights.keys())
        screener = filter_universe(universe)
    
    # compute past returns
    momentum = period_returns(lookback)
    pipe.add(momentum,'momentum')
    pipe.set_screen(screener)
    return pipe

def generate_signals(context, data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        context.weights = {}
        return
    
    n = context.params['num_stocks']
    candidates = pipeline_results.dropna().sort_values('momentum')
    size = len(candidates)

    if size == 0:
        print(f'{get_datetime()}, no stocks passed filterting criteria.')
        context.weights = {}
        
    if size < n:
        print(f'{get_datetime()}, only {size} stocks passed filterting criteria.')
        
    candidates = candidates[-n:]
    value = context.params['order_value']
    candidates['weights'] = value
    context.weights = candidates.weights.to_dict()

def rebalance(context,data):
    n = len(context.weights)
    if n < 1:
        return

    # square off old positions if any
    for security in context.portfolio.positions:
        if security not in context.weights:
               order_target_value(security, 0)

    # Place orders for the new portfolio
    for security in context.weights:
        order_target_value(security, context.weights[security])
        
    if context.dynamic:
        # reset current universe for dynamic selection
        context.weights = {}
        
    if context.mode != 'BACKTEST':
        print(context.mode)
        month, year = next_month(get_datetime())
        msg = 'Rebalancing orders sent. This strategy is designed for '
        msg += 'monthly rebalancing, next rebalance date is first '
        msg += f'business day of {month}, {year}'
        print(msg)
        terminate()