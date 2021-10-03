"""
    Title: Buy and hold strategy
    Description: This is a long only strategy which rebalces the 
        portfolio weights every month at month start.
    Style tags: Systematic
    Asset class: Equities, Futures, ETFs, Currencies and Commodities
    Dataset: NSE Daily or NSE Minute
    
    zipline:25s
    blueshift: 1.08s
"""

from blueshift.api import(    symbol,
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
                               symbol('ASIANPAINT'),
                               symbol('TCS')
                             ]
    
    # Call rebalance function on the first trading day of each month 2.5 hours before market close
    schedule_function(rebalance,
                    date_rules.month_start(days_offset=0),
                    time_rules.market_close(hours=2, minutes=30))


def rebalance(context,data):
    """
        A function to rebalance the portfolio, passed on to the call
        of schedule_function above.
    """

    # Position 50% of portfolio to be long in each security
    for security in context.long_portfolio:
        order_target_percent(security, 0.5)         
