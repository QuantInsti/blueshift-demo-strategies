"""
    Title: A naive (inverse vol-weighted) risk parity strategy with 
        monthly rebalance across styles/ factors/ asset classes
    Description: This strategy uses past returns to rank securities
                and go long (short) the top (bottom) n-percentile
    Style tags: Momentum
    Asset class: Equities, Futures, ETFs, Currencies
    Dataset: US Equities
"""
from blueshift_library.pipelines.pipelines import technical_factor
from blueshift_library.technicals.indicators import volatility

from blueshift.errors import NoFurtherDataError
from blueshift.pipeline import Pipeline, CustomFilter
from blueshift.api import symbol, sid, order_target_percent, get_datetime
from blueshift.api import schedule_function, date_rules, time_rules
from blueshift.api import attach_pipeline, pipeline_output

def filter_universe(universe):
    universe = [asset.symbol for asset in universe]
    class FilteredUniverse(CustomFilter):
        inputs = ()
        window_length = 1
        def compute(self,today,assets,out):
            in_universe = [sid(asset).symbol in universe for asset in assets]
            out[:] = in_universe
    return FilteredUniverse()

def initialize(context):
    context.lookback = 12*21
    context.offset = 1*21
    context.size = 5
    context.weight = 0
    context.candidates = []

    context.universe =      [
                               symbol('SPY'),  # large cap
                               symbol('QQQ'),  # tech
                               symbol('VUG'),  # growth
                               symbol('QUAL'), # quality
                               symbol('MTUM'), # momentum
                               symbol('IWM'),  # small cap
                               symbol('USMV'), # min vol                               
                               symbol('HDV'),  # dividend
                               symbol('VEU'),  # world equity
                               symbol('VWO'),  # EM equity
                               symbol('DBC'),  # commodities
                               symbol('USO'),  # oil
                               symbol('GLD'),  # gold
                               symbol('AGG'),  # bonds
                               symbol('TIP'),  # inflation
                             ]
    
    attach_pipeline(make_strategy_pipeline(context), name='strategy_pipeline')
    schedule_function(rebalance,
                    date_rules.month_start(days_offset=0),
                    time_rules.market_close(hours=2, minutes=30))


def make_strategy_pipeline(context):
    pipe = Pipeline()
    assets = [asset for asset in context.universe]
    screener = filter_universe(assets)
    pipe.add(
        technical_factor(context.lookback, volatility, 1),'vol')
    pipe.set_screen(screener)

    return pipe

def compute_signal(context,data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        print(f'{get_datetime()}: error in pipeline output.')
        context.candidates = []
        return

    candidates = pipeline_results.dropna()

    if len(candidates) < context.size:
        print(f'{get_datetime()}:no assets passed screening.')
        context.candidates = []

    candidates = candidates.tail(context.size)
    candidates['weight'] = 1/candidates.vol
    total = candidates.weight.sum()
    candidates['weight'] = candidates['weight']/total

    # naive risk parity - inverse vol weights
    context.weight = candidates['weight'].to_dict()
    context.candidates = candidates.index.tolist()


def rebalance(context,data):
    compute_signal(context, data)

    if not context.candidates:
        for asset in context.portfolio.positions:
            order_target_percent(asset, 0)
        return

    for asset in context.universe:
        if asset not in context.candidates:
            order_target_percent(asset, 0)
        else:
            order_target_percent(asset, context.weight[asset])
