"""
    Title: technical indicators library
    Description: a library with common technical indicators as wrapper 
        around the TA-lib. 
    Asset class: Any
    Dataset: Not applicable.
    Note: This cannot be run as is, but should be imported in other 
        strategy code. Use `from library.technicals.indicators import sma`
        for example, to import the `sma` functionality.
"""
import talib as ta
import numpy as np
import bisect

def volatility(px, lookback):
    """ 
        returns (non-annualized) returns volatility.

        Args:
            px (ndarray): input price array
            lookback (int): lookback window size
        Returns:
            volatility (float) of returns
    """
    returns = ta.ROC(px,lookback)/100
    returns = returns[~np.isnan(returns)]
    return np.std(returns)

def sma(px, lookback):
    """ 
        returns simple moving average.

        Args:
            px (ndarray): input price array
            lookback (int): lookback window size
        Returns:
            simple moving average (float) of prices
    """
    sig = ta.SMA(px, timeperiod=lookback)
    return sig[-1]

def ema(px, lookback):
    """ 
        returns exponential moving average.

        Args:
            px (ndarray): input price array
            lookback (int): lookback window size
        Returns:
            exponential moving average (float) of prices
    """
    sig = ta.EMA(px, timeperiod=lookback)
    return sig[-1]

def rsi(px, lookback):
    """ 
        returns relative strength index.

        Args:
            px (ndarray): input price array
            lookback (int): lookback window size
        Returns:
            RSI (float) of prices
    """
    sig = ta.RSI(px, timeperiod=lookback)
    return sig[-1]

def bollinger_band(px, lookback):
    """ 
        returns bollinger band computation with default values (2 standard
        deviation band-width).

        Args:
            px (ndarray): input price array
            lookback (int): lookback window size
        Returns:
            upper, mid and lower (tuple) bands levels.
    """
    upper, mid, lower = ta.BBANDS(px, timeperiod=lookback)
    return upper[-1], mid[-1], lower[-1]

def macd(px, lookback):
    """ 
        returns moving average convergence divergence computation with 
        default values.

        Args:
            px (ndarray): input price array
            lookback (int): lookback window size
        Returns:
            upper, mid and lower (tuple) bands levels.
    """
    macd_val, macdsignal, macdhist = ta.MACD(px)
    return macd_val[-1], macdsignal[-1], macdhist[-1]

def doji(px):
    """ 
        returns true if a DOJI candlestick pattern is found in the last
        price bar.

        Args:
            px (ndarray): input price array
        Returns:
            Bool, True if the last point completes a DOJI pattern.
    """
    sig = ta.CDLDOJI(px.open.values, px.high.values, px.low.values, px.close.values)
    return sig[-1]

def roc(px, lookback):
    """ 
        returns rate of change (ROC)

        Args:
            px (ndarray): input price array
            lookback (int): lookback window size
        Returns:
            Float, last value of the ROC.
    """
    signal = ta.ROC(px, timeperiod=lookback)
    return signal[-1]

def adx(px, lookback):
    """ 
        returns average directional index.

        Args:
            px (DataFrame): input price array with OHLC columns
            lookback (int): lookback window size
        Returns:
            Float, last value of the ADX.
    """
    signal = ta.ADX(px.high.values, px.low.values, px.close.values, timeperiod=lookback)
    return signal[-1]

def fibonacci_support(px):
    """ 
        Computes the current Fibonnaci support and resistance levels. 
        Returns the distant of the last price point from both.

        Args:
            px (ndarray): input price array
        Returns:
            Tuple, distance from support and resistance levels.
    """
    def fibonacci_levels(px):
        return [min(px) + l*(max(px) - min(px)) for l in [0,0.236,0.382,0.5,0.618,1]]

    def find_interval(x, val):
        return (-1 if val < x[0] else 99) if val < x[0] or val > x[-1] \
            else  max(bisect.bisect_left(x,val)-1,0)

    last_price = px[-1]
    lower_dist = upper_dist = 0
    sups = fibonacci_levels(px[:-1])
    idx = find_interval(sups, last_price)

    if idx==-1:
        lower_dist = -1
        upper_dist = round(100.0*(sups[0]/last_price-1),2)
    elif idx==99:
        lower_dist = round(100.0*(last_price/sups[-1]-1),2)
        upper_dist = -1
    else:
        lower_dist = round(100.0*(last_price/sups[idx]-1),2)
        upper_dist = round(100.0*(sups[idx+1]/last_price-1),2)

    return lower_dist,upper_dist

