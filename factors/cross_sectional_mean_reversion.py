"""
    Title: Classic (Jegadeesh 1990) cross-sectional mean reversion
    Description: This strategy uses short term reversal anomaly
        (over a week) to calculate excess returns over a broad 
        index and go long the losers and short the winners.
    reference: http://finance.martinsewell.com/stylized-facts/dependence/Jegadeesh1990.pdf
    seealso: http://quantpedia.com/strategies/short-term-reversal-in-stocks/
    Style tags: Momentum
    Asset class: Equities, Futures, ETFs, Currencies
    Dataset: All
"""

import numpy as np

from blueshift_library.pipelines.pipelines import average_volume_filter, period_returns

from blueshift.pipeline import Pipeline, CustomFactor
from blueshift.pipeline.data import EquityPricing
from blueshift.errors import NoFurtherDataError
from blueshift.finance import commission, slippage
from blueshift.api import(  symbol,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            attach_pipeline,
                            pipeline_output,
                            set_commission,
                            set_slippage,
                       )

def liquidity_factor(lookback, amount):
    """
        dollar-weighted average volume as the liquidity factor, 
        used to filter tradeable universe.
    """
    class AvgDailyDollarVolumeTraded(CustomFactor):
        inputs = [EquityPricing.close, EquityPricing.volume]
        def compute(self,today,assets,out,close_price,volume):
            dollar_volume = np.nanmean(close_price * volume, axis=0)
            out[:] = dollar_volume
    
    return AvgDailyDollarVolumeTraded(window_length = lookback)

def initialize(context):
    """
        function to define things to do at the start of the strategy
    """

    context.weights = {} # the weights to trade
    # strategy parameters
    context.params = {'lookback_vol':252,
                      'lookback_ret':5, 
                      'percentile':0.05,
                      'min_volume':1E8,
                      'universe':100,
                      }
    
    # Call rebalance function on the first trading day of each week
    schedule_function(strategy, date_rules.week_start(), 
            time_rules.market_close(minutes=30))

    # Set up the pipeline
    attach_pipeline(make_strategy_pipeline(context), 
            name='strategy_pipeline')

    # uncomment below two lines to exclude slippage and costs
    # set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
    # set_slippage(slippage.FixedSlippage(0.00))

def strategy(context, data):
    generate_signals(context, data)
    rebalance(context,data)

def make_strategy_pipeline(context):
    pipe = Pipeline()
    lookback = context.params['lookback_vol']
    v = context.params['min_volume']
    
    # get the filters and factors
    volume_filter = average_volume_filter(lookback, v)
    momentum = period_returns(context.params['lookback_ret'])
    liquidity = liquidity_factor(lookback, v)

    # set up the pipeline
    pipe.add(momentum,'momentum')
    pipe.add(liquidity,'liquidity')
    pipe.set_screen(volume_filter)

    return pipe

def generate_signals(context, data):
    try:
        pipeline_results = pipeline_output('strategy_pipeline')
    except NoFurtherDataError:
        context.weights = {}
        return
    
    # return if the filtered universe is too short
    n = int(context.params['universe'])
    rets = pipeline_results.dropna()
    if len(rets) < n:
        context.weights = {}
        return

    # select top n assets as universe by liquidity factor
    rets = rets.sort_values('liquidity')
    rets = rets.iloc[-n:]

    # calculate the 1-week broad market return
    spx = data.history(symbol('SPY'),'close',10,'1d').dropna()
    mkt_ret = spx[-1]/spx[-5] - 1

    # calculate mean reversion weights
    rets['weight'] = -(rets['momentum']-mkt_ret)
    rets['weight'] = rets['weight']/rets['weight'].abs().sum()
    context.weights = rets['weight'].to_dict()


def rebalance(context,data):
    # square off old positions if any
    for asset in context.portfolio.positions:
        if asset not in context.weights:
            order_target_percent(asset, 0)

    # Place orders for the new portfolio
    for asset in context.weights:
        order_target_percent(asset, context.weights[asset])
