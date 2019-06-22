import numpy as np

from zipline.pipeline import Pipeline, CustomFilter, CustomFactor
from zipline.pipeline.data import EquityPricing

def average_volume_filter(lookback, amount):
    class AvgDailyDollarVolumeTraded(CustomFilter):
        inputs = [EquityPricing.close, EquityPricing.volume]
        def compute(self,today,assets,out,close_price,volume):
            dollar_volume = np.mean(close_price * volume, axis=0)
            high_volume = dollar_volume > amount
            out[:] = high_volume
    return AvgDailyDollarVolumeTraded(window_length = lookback)

def period_returns(lookback):
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            start_price = close_price[0]
            end_price = close_price[-1]
            returns = end_price/start_price - 1
            out[:] = returns
    return SignalPeriodReturns(window_length = lookback)

def period_returns2(lookback, volume_filter):
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            start_price = close_price[0]
            end_price = close_price[-1]
            returns = end_price/start_price - 1
            out[:] = returns
    return SignalPeriodReturns(window_length = lookback, mask=volume_filter)

def technical_factor(lookback, indicator_fn, indicator_lookback):
    class SignalPeriodReturns(CustomFactor):
        inputs = [EquityPricing.close]
        def compute(self,today,assets,out,close_price):
            signals = np.apply_along_axis(
                indicator_fn, 0, close_price, indicator_lookback)
            out[:] = signals
    return SignalPeriodReturns(window_length = lookback)