"""
    Title: Demo strategy for Algo states
    Description: A demo strategy to explain how to access algo states like portfolio 
        details and account statistics
    Asset class: All
    Dataset: US Equities
    
    Run this example for a few days (say two or three days) with the 
    NSE daily data set and examine the output in the Logs tab.
"""
from blueshift.api import(  symbol,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            get_datetime,
                       )

def initialize(context):
    """
        A function to define things to do at the start of the strategy
    """
    
    # universe selection
    context.universe = [
                               symbol('AMZN'),
                               symbol('FB'),
                               symbol('AAPL'),
                               symbol('GOOGL'),
                               symbol('NFLX'),
                             ]
    
    # Call rebalance function on the first trading day of each month after 2.5 hours from market open
    schedule_function(rebalance,
                    date_rules.month_start(days_offset=0),
                    time_rules.market_close(hours=2, minutes=30))


def rebalance(context,data):
    """
        A function to rebalance the portfolio, passed on to the call
        of schedule_function above.
    """

    for security in context.universe:
        order_target_percent(security, 1.0/10)

def analyze(context, perf):
    """
        Called at the end of strategy run.
    """
    # current simulation date-time
    print('{} {}'.format(get_datetime().date(), 30*'#'))

    # accessing portfolio details
    portfolio_value = context.portfolio.portfolio_value
    cash = context.portfolio.cash
    positions = context.portfolio.positions
    print('portfolio_value {}, cash {}'.format(portfolio_value, cash))

    for asset, position in positions.items():
        print('Symbol {}, amount {}'.format
              (asset.symbol, position.quantity))

    # accessing account details
    print('leverage {}'.format(context.account.leverage))
    print('net leverage {}'.format(context.account.net_leverage))
    print('available cash {}'.format(context.account.available_funds))
    print('total positions exposure {}'.format(context.account.total_positions_exposure))
    
    print('performance data columns...')
    print(perf.columns)