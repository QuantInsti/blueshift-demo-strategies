"""
Custom pipeline filters and factors examples
"""

import numpy as np
import warnings

try:
    from blueshift.data.pipeline.data import EquityPricing
    from blueshift.pipeline import CustomFilter, CustomFactor
    from blueshift.pipeline.factors import AverageDollarVolume
except ImportError:
    raise ValueError('pipeline is not supported on this version of blueshift.')
    
def select_universe(lookback, size, context=None):
    """
       Returns a custom filter object for volume-based filtering.
       
       Args:
           `lookback (int)`: lookback window size
           `size (int)`: Top n assets to return.
           
       Returns:
           A custom filter object
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import select_universe
           
           pipe = Pipeline()
           top_100 = select_universe(252, 100)
           pipe.set_screen(top_100)
    """
    return AverageDollarVolume(window_length=lookback).top(size)

def average_volume_filter(lookback, amount, context=None):
    """
       Returns a custom filter object for volume-based filtering.
       
       Args:
           `lookback (int)`: lookback window size
           `amount (int)`: amount to filter (high-pass)
           
       Returns:
           A custom filter object
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import average_volume_filter
           
           # then inside the pipeline builder function
           pipe = Pipeline()
           volume_filter = average_volume_filter(200, 1000000)
           pipe.set_screen(volume_filter)
    """
    class AvgDailyDollarVolumeTraded(CustomFilter):
        inputs = [EquityPricing.close, EquityPricing.volume]
        def compute(self,today,assets,out,close_price,volume):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                dollar_volume = np.nanmean(close_price * volume, axis=0)
            high_volume = dollar_volume > amount
            out[:] = high_volume
    
    return AvgDailyDollarVolumeTraded(window_length = lookback)

def average_volume_factor(lookback, amount, context=None):
    """
       Returns a custom factor object for volume-based filtering.
       
       Args:
           `lookback (int)`: lookback window size
           `amount (int)`: amount to filter (high-pass)
           
       Returns:
           A custom factor object
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import average_volume_filter
           
           # then inside the pipeline builder function
           pipe = Pipeline()
           volume_factor = average_volume_factor(200, 1000000)
           pipe.add(average_volume_factor, 'volume')
    """
    class AvgDailyDollarVolumeTraded(CustomFactor):
        inputs = [EquityPricing.close, EquityPricing.volume]
        def compute(self,today,assets,out,close_price,volume):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                dollar_volume = np.nanmean(close_price * volume, axis=0)
            out[:] = dollar_volume
    
    return AvgDailyDollarVolumeTraded(window_length = lookback)

def filter_assets(func=None, context=None):
    """
       Returns a custom filter object to filter assets based on a user 
       supplied function. The function must return `True` for assets 
       that are selected and `False` for assets to be filtered out. It 
       should accept a single argument (an asset object).
       
       Args:
           `func (callable)`: A function for filtering.
           
       Returns:
           A custom filter object
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import filter_assets
           # from blueshift.assets import Equity
           
           # then inside the pipeline builder function
           pipe = Pipeline()
           # filter out non-Equity assets
           func = lambda asset:isinstance(asset, Equity)
           asset_filter = filter_assets(func, context)
           pipe.set_screen(asset_filter)
    """
    if not context:
        msg = f'Missing context, pass a valid context as keyword argument.'
        raise ValueError(msg)
        
    symbol_fn = context.get_algo().symbol
    sid_fn = context.get_algo().sid
    if func is None:
        func = lambda asset:True

    class FilteredUniverse(CustomFilter):
        inputs = ()
        window_length = 1
        def compute(self,today,assets,out):
            # we do a symbol(sid().symbol) here as sid may not be same 
            # between the pipeline store and the active store
            in_universe = [func(symbol_fn(sid_fn(asset).symbol)) for asset in assets]
            out[:] = in_universe
    
    return FilteredUniverse()

def filter_universe(universe, context=None):
    """
       Returns a custom filter object to filter based on a user 
       supplied list of assets objects. This is useful where we still 
       want to use the underlying pipeline computation facilities, but 
       want to specify assets explicitly.
       
       Args:
           `universe (list)`: A list of asset objects to keep.
           
       Returns:
           A custom filter object
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import filter_universe
           
           # define the universe in the `initialize` function
           # context.universe = [symbol("AAPL"), symbol("MSFT")]
           
           # then inside the pipeline builder function
           pipe = Pipeline()
           universe_filter = filter_universe(context.universe, context)
           pipe.set_screen(universe_filter)
    """
    if not context:
        msg = f'Missing context, pass a valid context as keyword argument.'
        raise ValueError(msg)
        
    sid_fn = context.get_algo().sid
    
    universe = frozenset([asset.exchange_ticker for asset in universe])
    class FilteredUniverse(CustomFilter):
        inputs = ()
        window_length = 1
        def compute(self,today,assets,out):
            # we do a sid().symbol here as sid may not be same between
            # the pipeline store and the active store
            in_universe = [sid_fn(asset).exchange_ticker in universe for asset in assets]
            out[:] = in_universe
    
    return FilteredUniverse()

def exclude_assets(universe, context=None):
    """
       Returns a custom filter object to filter based on a user 
       supplied list of assets objects to exclude.
       
       Args:
           `universe (list)`: A list of asset objects to exclude.
           
       Returns:
           A custom filter object.
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import filter_universe
           
           # define assets to exclude in the `initialize` function
           # context.exclude = [symbol("AAPL"), symbol("MSFT")]
           
           # then inside the pipeline builder function
           pipe = Pipeline()
           exclude_filter = filter_universe(context.exclude, context)
           pipe.set_screen(exclude_filter)
    """
    if not context:
        msg = f'Missing context, pass a valid context as keyword argument.'
        raise ValueError(msg)
        
    sid_fn = context.get_algo().sid
    
    universe = frozenset([asset.exchange_ticker for asset in universe])
    class FilteredUniverse(CustomFilter):
        inputs = ()
        window_length = 1
 
        def compute(self,today,assets,out):
            # we do a sid here as sid().symbol may not be same between
            # the pipeline store and the active store
            in_universe = [sid_fn(asset).exchange_ticker not in universe for asset in assets]
            out[:] = in_universe
 
    return FilteredUniverse()

def returns_factor(lookback, offset=0, context=None):
    """
       Returns a custom factor object for computing simple returns over
       a period (`lookback`).
       
       Args:
           `lookback (int)`: lookback window size
           `offset (int)`: offset from the end of the window
           
       Returns:
           A custom factor object.
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import returns_factor
           
           # then inside the pipeline builder function
           pipe = Pipeline()
           momentum = returns_factor(200)
           pipe.add(momentum,'momentum')
    """
    if offset >= lookback:
        raise ValueError(f'Offset must be less than lookback, got {offset}, {lookback}')
        
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            start_price = close_price[0]
            end_price = close_price[-(1+offset)]
            returns = end_price/start_price - 1
            out[:] = returns
    
    return SignalPeriodReturns(window_length = lookback)

def filtered_returns_factor(lookback, filter_, offset=0, context=None):
    """
       Returns a custom factor object for computing simple returns over
       a period (`lookback`), with a volume filter applied. Equivalent to separately
       applying `period_returns` and `average_volume_filter` above.
       
       Args:
           `lookback (int)`: lookback window size
           `filter_ (CustomFilter)`: a custom volume filter
           `offset (int)`: offset from the end of the window
           
       Returns:
           A custom factor object.
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import average_volume_filter, period_returns2
           
           # then inside the pipeline builder function
           pipe = Pipeline()
           volume_filter = average_volume_filter(200, 1000000)
           momentum = filtered_returns_factor(200,volume_filter)
           pipe.add(momentum,'momentum')
    """
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            start_price = close_price[0]
            end_price = close_price[-(1+offset)]
            returns = end_price/start_price - 1
            out[:] = returns
    
    return SignalPeriodReturns(window_length = lookback, 
                               mask=filter_)

def technical_factor(lookback, indicator_fn, indicator_lookback=None,
                     context=None):
    """
       A factory function to generate a custom factor by applying a 
       user-defined function over asset closing prices.
       
       Args:
           `lookback (int)`: lookback window size
           `indicator_fn (function)`: user-defined function
           `indicator_lookback (int)`: lookback for user-defined function.
           
       Returns:
           A custom factor object applying the supplied function.
           
       Note:
           The `indicator_fn` must be of the form f(px, n), where
           px is numpy ndarray and lookback is an n. Also the `lookback` 
           argument above must be greater than or equal to the other 
           argument `indicator_lookback`. If `None` it is set as the 
           same value of `lookback`.
           
       .. code-block:: python
           
           # from blueshift.library.pipelines.pipelines import technical_factor
           # from blueshift.library.technicals.indicators import rsi 
           
           # then inside the pipeline builder function
           pipe = Pipeline()
           rsi_factor = technical_factor(14, rsi)
           pipe.add(rsi_factor,'rsi')
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

