"""
    Demo for accounts and portfolio objects using the context variable.
    See https://blueshift.quantinsti.com/api-docs/context.html#context-object
    for mode details. Run it for a couple of months starting with the 
    first date of any month for the US-Equities dataset.
"""
from blueshift.api import symbol, order_target_percent, schedule_function, date_rules, time_rules


def initialize(context):
    """
        A function to define things to do at the start of the strategy
    """
    context.universe = [symbol('AMZN'), symbol('MSFT'), symbol('AAPL'),
        symbol('GOOGL'), symbol('NFLX')]
    schedule_function(rebalance, date_rules.month_start(days_offset=0),
        time_rules.market_close(hours=2, minutes=30))


def rebalance(context, data):
    """
        A function to rebalance the portfolio, passed on to the call
        of schedule_function above.
    """
    for security in context.universe:
        order_target_percent(security, 1.0 / 10)


def analyze(context, perf):
    # see https://blueshift.quantinsti.com/api-docs/objects.html#assets
    AMZN = symbol('AMZN')
    print(AMZN.to_dict())

    # see https://blueshift.quantinsti.com/api-docs/context.html#portfolio-and-account
    print(context.account.gross_leverage)
    print(context.portfolio.portfolio_value)

    # see https://blueshift.quantinsti.com/api-docs/objects.html#positions
    positions = context.portfolio.positions
    for asset in positions:
        print(f'position for asset {asset}...')
        print(positions[asset].to_dict())
        break
