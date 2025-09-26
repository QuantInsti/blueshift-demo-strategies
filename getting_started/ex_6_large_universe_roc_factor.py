"""
    Demo for running strategies with a large number of assets - where it is 
    impractical to manually define the universe of instruments we are interested 
    in. This demonstrates the use of the powerful `pipeline` APIs to 
    programatically filter the available universe based on pre-define (or even 
    custom) filters and compute (pre-defined or custom) factors on them for 
    further decision making. Note the main elements of the pipeline APIs -> a 
    function (see `make_screener` below) that defines the pipeline object and 
    adds filters and/ or factors to it; a call to the `attach_pipeline` function 
    to register the pipeline with the current strategy for later evaluation; 
    and finally calling `pipeline_output` to get the results from the pipeline.
    Pipelines are lazy computation graph objects that run computation only 
    when required - an efficient way for running custom query on large asset 
    universe.
"""
from blueshift.library.pipelines.pipelines import average_volume_filter, technical_factor
from blueshift.library.technicals.indicators import roc

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
                      'size':5,
                      'min_volume':1E7
                      }
    
    # Call rebalance function on the first trading day of each month
    schedule_function(run_strategy, date_rules.month_start(), 
            time_rules.market_close(minutes=30))

    # Set up the pipe-lines for strategies
    attach_pipeline(make_screener(context), name='my_screener')

def make_screener(context):
    pipe = Pipeline()

    # get the strategy parameters
    lookback = context.params['lookback']*21
    v = context.params['min_volume']

    # Set the volume filter
    volume_filter = average_volume_filter(lookback, v)
    
    # compute past returns
    roc_factor = technical_factor(lookback+5, roc, lookback)
    
    pipe.add(roc_factor,'roc')
    roc_filter = roc_factor > 0
    pipe.set_screen(roc_filter & volume_filter)

    return pipe

def screener(context, data):
    try:
        pipeline_results = pipeline_output('my_screener')
    except NoFurtherDataError:
        print('no pipeline for {}'.format(get_datetime()))
        return []

    pipeline_results = pipeline_results.dropna()
    selected = pipeline_results.sort_values(
        'roc')[-(context.params['size']):]
    return selected.index.tolist()

def run_strategy(context, data):
    assets = screener(context, data)
    current_holdings = context.portfolio.positions.keys()

    exits = set(current_holdings) - set(assets)
    for asset in exits:
        order_target_percent(asset, 0)

    if assets:
        sizing = 1.0/len(assets)
        for asset in assets:
            order_target_percent(asset, sizing)
    
