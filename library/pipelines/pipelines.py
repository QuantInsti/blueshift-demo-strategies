"""
    Title: Custom factors and filters library
    Description: a library with common filters/ factors
    Asset class: Any
    Dataset: Not applicable.
    Note: This cannot be run as is, but should be imported in other 
        strategy. Use `from pipelines.pipelines import period_returns`.
        Also, these (and any pipeline functionalities) are **NOT** 
        accessible in live runs.
"""
import numpy as np

from blueshift.pipeline import CustomFilter, CustomFactor
from blueshift.pipeline.data import EquityPricing
from blueshift.pipeline.factors import AverageDollarVolume

ENGINE = 'blueshift'

def select_universe(lookback, size):
    """
       Returns a custom filter object for volume-based filtering.
       
       Args:
           lookback (int): lookback window size
           size (int): Top n assets to return.
           
       Returns:
           A custom filter object
           
       Examples::
           
           # from library.pipelines.pipelines import select_universe
           
           pipe = Pipeline()
           top_100 = select_universe(252, 100)
           pipe.set_screen(top_100)
    """
    return AverageDollarVolume(window_length=lookback).top(size)
    
def average_volume_filter(lookback, amount):
    """
       Returns a custom filter object for volume-based filtering.
       
       Args:
           lookback (int): lookback window size
           amount (int): amount to filter (high-pass)
           
       Returns:
           A custom filter object
           
       Examples::
           
           # from library.pipelines.pipelines import average_volume_filter
           
           pipe = Pipeline()
           volume_filter = average_volume_filter(200, 1000000)
           pipe.set_screen(volume_filter)
    """
    class AvgDailyDollarVolumeTraded(CustomFilter):
        inputs = [EquityPricing.close, EquityPricing.volume]
        def compute(self,today,assets,out,close_price,volume):
            dollar_volume = np.mean(close_price * volume, axis=0)
            high_volume = dollar_volume > amount
            out[:] = high_volume
    
    return AvgDailyDollarVolumeTraded(window_length = lookback)

def filter_universe(universe):
    """
       Returns a custom filter object to filter based on a user 
       supplied list of assets objects.
       
       Args:
           universe (list): A list of asset objects to keep.
           
       Returns:
           A custom filter object.
           
       Examples::
           
           # from library.pipelines.pipelines import filter_universe
           # context.universe = [symbol(AAPL), symbol(MSFT)]
           
           pipe = Pipeline()
           universe_filter = filter_universe(context.universe)
           pipe.set_screen(universe_filter)
    """
    from blueshift.api import sid
    
    universe = frozenset([asset.symbol for asset in universe])
    class FilteredUniverse(CustomFilter):
        inputs = ()
        window_length = 1
        def compute(self,today,assets,out):
            # we do a sid().symbol here as sid may not be same between
            # the pipeline store and the active store
            in_universe = [sid(asset).symbol in universe for asset in assets]
            out[:] = in_universe
    
    return FilteredUniverse()

def exclude_assets(universe):
    """
       Returns a custom filter object to filter based on a user 
       supplied list of assets objects to exclude.
       
       Args:
           universe (list): A list of asset objects to exclude.
           
       Returns:
           A custom filter object.
           
       Examples::
           
           # from library.pipelines.pipelines import filter_universe
           # context.exclude = [symbol(AAPL), symbol(MSFT)]
           
           pipe = Pipeline()
           exclude_filter = filter_universe(context.exclude)
           pipe.set_screen(exclude_filter)
    """
    from blueshift.api import sid
    
    universe = frozenset([asset.symbol for asset in universe])
    class FilteredUniverse(CustomFilter):
        inputs = ()
        window_length = 1
 
        def compute(self,today,assets,out):
            # we do a sid here as sid().symbol may not be same between
            # the pipeline store and the active store
            in_universe = [sid(asset).symbol not in universe for asset in assets]
            out[:] = in_universe
 
    return FilteredUniverse()

def period_returns(lookback):
    """
       Returns a custom factor object for computing simple returns over
       period.
       
       Args:
           lookback (int): lookback window size
           
       Returns:
           A custom factor object.
           
       Examples::
           
           # from library.pipelines.pipelines import period_returns
           pipe = Pipeline()
           momentum = period_returns(200)
           pipe.add(momentum,'momentum')
    """
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            start_price = close_price[0]
            end_price = close_price[-1]
            returns = end_price/start_price - 1
            out[:] = returns
    
    return SignalPeriodReturns(window_length = lookback)

def period_returns2(lookback, volume_filter):
    """
       Returns a custom factor object for computing simple returns over
       period, with a volume filter applied. Equivalent to separately
       applying `period_returns` and `average_volume_filter` above.
       
       Args:
           lookback (int): lookback window size
           
       Returns:
           A custom factor object.
           
       Examples::
           
           # from library.pipelines.pipelines import average_volume_filter, period_returns2
           
           pipe = Pipeline()
           volume_filter = average_volume_filter(200, 1000000)
           momentum = period_returns2(200,volume_filter)
           pipe.add(momentum,'momentum')
    """
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            start_price = close_price[0]
            end_price = close_price[-1]
            returns = end_price/start_price - 1
            out[:] = returns
    
    return SignalPeriodReturns(window_length = lookback, 
                               mask=volume_filter)

def technical_factor(lookback, indicator_fn, indicator_lookback=None):
    """
       A factory function to generate a custom factor by applying a 
       user-defined function over asset returns.
       
       Args:
           lookback (int): lookback window size
           indicator_fn (function): user-defined function
           indicator_lookback (int): lookback for user-defined function.
           
       Returns:
           A custom factor object applying the supplied function.
           
       Note:
           The `indicator_fn` must be of the form f(px, n), where
           px is numpy ndarray and lookback is an n. Also `lookback` 
           argument above must be greater than or equal to the other 
           argument `indicator_lookback`. If `None` it is set as the 
           same value of `lookback`.
           
       Examples::
           
           # from library.pipelines.pipelines import technical_factor
           
           pipe = Pipeline()
           rsi_factor = technical_factor(14, rsi)
           pipe.add(momentum,'momentum')
    """
    if indicator_lookback is None:
        indicator_lookback = lookback
    
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            signals = np.apply_along_axis(
                indicator_fn, 0, close_price, indicator_lookback)
            out[:] = signals
    
    return SignalPeriodReturns(window_length = lookback)

