"""
    Title: Short Dollar Basket
    Description: This is a long only strategy which rebalces the 
        portfolio weights every month at month start.
    Style tags: Macro
    Asset class: Currencies
    Dataset: FX Minute
"""
from blueshift.api import(  symbol,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            set_account_currency
                       )

def initialize(context):
    """
        A function to define things to do at the start of the strategy
    """
    # set the account currency, only valid for backtests
    set_account_currency("USD")
    
    # universe selection
    context.short_dollar_basket = {
                               symbol('AUD/USD'):1,
                               symbol('EUR/USD'):1,
                               symbol('GBP/USD'):1,
                               symbol('NZD/USD'):1,
                               symbol('USD/CAD'):-1,
                               symbol('USD/CHF'):-1,
                               symbol('USD/JPY'):-1,
                             }
    
    # Call rebalance function on the first trading day of each month after 2.5 hours from market open
    schedule_function(rebalance,
                    date_rules.month_start(days_offset=0),
                    time_rules.market_close(hours=2, minutes=30))


def rebalance(context,data):
    """
        A function to rebalance the portfolio, passed on to the call
        of schedule_function above.
    """

    # Position equally
    for security in context.short_dollar_basket:
        w = round(context.short_dollar_basket[security]/7,2)
        order_target_percent(security, w)
