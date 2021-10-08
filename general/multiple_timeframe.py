"""
    Title: Buy and Hold (NSE)
    Description: This is a demo strategy to show how to use multiple 
        time-frames in blueshift using Pandas resample method.
    Style tags: Systematic
    Asset class: Equities, Futures, ETFs, Currencies and Commodities
    Dataset: NSE Minute
"""
from blueshift.api import(  symbol,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                       )

def initialize(context):
    """
        A function to define things to do at the start of the strategy
    """
    
    # universe selection
    context.long_portfolio = [
                               symbol('MARUTI'),
                               symbol('HDFCBANK'),
                               symbol('TCS')
                             ]
    
    # Call rebalance function on the first trading day of each month after 2.5 hours from market open
    schedule_function(rebalance,
                    date_rules.month_start(days_offset=0),
                    time_rules.market_close(hours=0, minutes=30))


def rebalance(context,data):
    """
        A function to rebalance the portfolio, passed on to the call
        of schedule_function above.
    """
    prices = data.history(
        context.long_portfolio, 
        ['open','high','low','close', 'volume'], 750, '1m')

    for security in context.long_portfolio:
        # aggregate 15 minutes, all OHLCV columns
        df = to_period(prices.xs(security), period='15T')
        print(df.tail())

    print('-'*50)
    
    for security in context.long_portfolio:
        # aggregate 30 minutes, only two columns
        df = to_period(prices.xs(security)[['open','close']], period='30T')
        print(df.tail())

    print('-'*50)

    for security in context.long_portfolio:
        # aggregate hourly, only a single column
        df = to_period(prices.xs(security)['close'], period='60T')
        print(df.tail())

def to_period(price, period):
    """ 
        Define period as Pandas style, e.g. '30T' for 30-minutes aggregation.
        For a full list of Pandas offset abbr. see the link:
        https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases
    """
    if period.endswith('D'):
        label = closed = "left"
    else:
        label = closed = "left"

    try:
        mapper = {}
        if 'close' in price.columns:
            mapper['close'] = 'last'
        if 'open' in price.columns:
            mapper['open'] = 'first'
        if 'high' in price.columns:
            mapper['high'] = 'max'
        if 'low' in price.columns:
            mapper['low'] = 'min'
        if 'volume' in price.columns:
            mapper['volume'] = 'sum'
        out = price.resample(
                    period, label=label, closed=closed).agg(mapper)
    except:
        out = price.resample(
                    period, label=label, closed=closed).last()
    
    return out.fillna(method='ffill')
