"""
    Title: Long-only timeseries momentum hedged with broad market index.
    Description: This strategy uses past returns to rank securities
                and go long (short) the top (bottom) n-percentile
    Style tags: Momentum
    Asset class: Equities, Futures, ETFs, Currencies
    Dataset: US Equities or NSE
"""
from blueshift_library.pipelines.pipelines import average_volume_filter, period_returns, technical_factor
from blueshift_library.technicals.indicators import volatility

from blueshift.pipeline import Pipeline, CustomFilter
from blueshift.assets import InstrumentType
from blueshift.errors import NoFurtherDataError
from blueshift.api import sid, symbol, order, order_target_percent, get_datetime
from blueshift.api import schedule_function, date_rules, time_rules
from blueshift.api import attach_pipeline, pipeline_output
from blueshift.api import order_target_value

def filter_assets(func=None):
    if func is None:
        func = lambda asset:True

    class FilteredUniverse(CustomFilter):
        inputs = ()
        window_length = 1
        def compute(self,today,assets,out):
            in_universe = [func(sid(asset)) for asset in assets]
            out[:] = in_universe
    
    return FilteredUniverse()

def initialize(context):
    context.lookback = 12*21
    context.offset = 1*21
    context.min_volume = 1E8
    context.max_size = 10
    context.min_size = 5
    context.weight = 0

    context.universe = []
    if context.broker.name=='regT':
        context.hedge = symbol('SPY')
    elif context.broker.name=='nse-backtest':
        context.hedge = symbol('NIFTY-I')
    else:
        raise ValueError(f'this broker not supported:{context.broker.name}')
    
    context.hedge_threshold = 10000

    schedule_function(strategy, date_rules.month_start(days_offset=0), time_rules.market_close(hours=2, minutes=30))
    attach_pipeline(make_strategy_pipeline(context), name='strategy_pipeline')
    schedule_function(hedge, date_rules.every_day(), time_rules.market_open(hours=0, minutes=30))
    schedule_function(hedge, date_rules.every_day(), time_rules.market_close(hours=0, minutes=30))

def make_strategy_pipeline(context):
    pipe = Pipeline()
    func = lambda asset:asset.instrument_type != InstrumentType.FUNDS
    asset_filter = filter_assets(func)
    volume_filter = average_volume_filter(context.lookback, context.min_volume)
    screener = asset_filter & volume_filter
    screener = volume_filter

    pipe.add(
        period_returns(context.lookback, context.offset),'momentum')
    pipe.add(
        technical_factor(context.lookback, volatility, 1),'vol')
    pipe.set_screen(screener)

    return pipe

def strategy(context, data):
    compute_signal(context, data)
    compute_weights(context, data)
    rebalance(context, data)

def compute_signal(context,data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        print(f'{get_datetime()}: error in pipeline output.')
        context.universe = []
        return

    momentum = pipeline_results
    momentum['z_score'] = momentum['momentum']/momentum['vol']
    candidates = momentum[momentum['z_score'] > 0].dropna().sort_values('z_score')
    n = context.max_size

    if len(candidates) < context.min_size:
        print(f'{get_datetime()}:no securities passed screening.')

    context.universe = candidates.index[-n:].tolist()

def compute_weights(context, data):
    if not context.universe:
        context.weight = 0
        return
    
    context.weight = 0.5/len(context.universe)

def rebalance(context, data):
    if not context.universe:
        for asset in context.portfolio.positions:
            order_target_percent(asset, 0)
        order_target_percent(context.hedge, 0)
        return

    for asset in context.portfolio.positions:
        if asset not in context.universe and asset != context.hedge:
            order_target_percent(asset, 0)

    for asset in context.universe:
        order_target_percent(asset, context.weight)

    hedge_weight = len(context.universe)*context.weight
    order_target_percent(context.hedge, hedge_weight)

def hedge(context, data):
    exposure = context.account.net_exposure

    if exposure < context.hedge_threshold:
        return

    if context.hedge in context.portfolio.positions:
        pos = context.portfolio.positions[context.hedge]
        exposure = exposure - pos.get_exposure()

    order_target_value(context.hedge, -exposure)
        
