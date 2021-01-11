"""
    Title: Realtime Stoploss/ Take-profit strategy in Forex
    Description: A strategy that enters a trade at the onset and set up
        two functions to monitor the stoploss and take-profit targets. If
        any is hit, squares off the position and terminate the algorithm.
        This works in live version only!!!
    Asset class: Any
    Dataset: Not applicable
    Note: this works in blueshift live version only!!!
"""
########################################################################
# PLACING TRADE REALTIME CAN SEND TOO MANY TRADES, PLEASE CEHCK YOUR 
# STRATEGY LOGIC CAREFULLY. TARGETTING FUNCTION DOES NOT CHECK FOR 
# PENDING ORDERS SO MAY NOT WORK AS EXPECTED IF TRADES ARE PLACED 
# AT A HIGH RATE. USE AN ON/OFF VARIABLE TO CONTROL PLACING OF TRADES.
# THE FOLLOWING EXAMPLES DO NOT PLACE REPEATED TRADES IN THE HANDLER.
########################################################################

from blueshift.api import symbol, order_target, get_datetime, terminate
from blueshift.api import on_data, off_data
from functools import partial

def initialize(context):
    """ this function is called once at the start of the execution. """
    context.universe = [symbol('NTPC')]
    context.ordered = {asset:False for asset in context.universe}
    context.target = {asset:0 for asset in context.universe}
    context.stop = {asset:0 for asset in context.universe}
    context.data_monitors = {}
    
def print_msg(msg):
    msg = f'{get_datetime()}: ' + msg
    print(msg)
    
def place_order(asset, context, price):
    context.target[asset] = price*1.005
    context.stop[asset] = price*0.995
    order_target(asset, 1, limit_price=price)
    context.ordered[asset] = True

def check_if_traded(asset, context, data):
    if asset in context.data_monitors:
        # already monitor setup
        return
    
    if asset in context.portfolio.positions:
        # check if order done, and setup exit monitor
        # does not handle partial fill, for that see NYSE realtime example
        print_msg(f'asset {asset} traded, will set up sl tp monitor.')
        context.data_monitors[asset] = partial(check_sl_tp, asset)
        return on_data(context.data_monitors[asset])
    
    print_msg(f'order for asset {asset} is still open.')
        
def check_sl_tp(asset, context, data):
    """ this function is called on every data update. """
    px = data.current(asset, 'close')
    if px > context.target[asset] or px < context.stop[asset]:
        if px > context.target[asset]:
            print_msg(
                    f'book profit on asset {asset} and stop sl tp monitor.')
        else:
            print_msg(
                    f'got stopped out on asset {asset} and stop sl tp monitor.')
        
        order_target(asset, 0)
        off_data(context.data_monitors[asset])
        terminate()


def handle_data(context,data):
    """ this function is called every minute. """
    print_msg(f'existing positions {context.portfolio.positions}')
    px = data.current(context.universe, 'close')
    
    for asset in context.universe:
        print_msg(f'for asset {asset}, status is {context.ordered[asset]}')
        if context.ordered[asset]:
            # if the broker has no support for order updates (`on_trade`), 
            # we check the order fill in handle_data, compare this with the
            # `check_order` function in the NYSE example
            return check_if_traded(asset, context, data)
        else:
            print_msg(f'entering {asset} at {px[asset]}.')
            place_order(asset, context, px[asset])