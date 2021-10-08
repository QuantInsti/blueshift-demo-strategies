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

from functools import partial
from blueshift.api import (symbol, order_target, get_datetime, terminate,
                           on_data, on_trade, off_data, off_trade)

def print_msg(msg):
    msg = f'{get_datetime()}:' + msg
    print(msg)

def check_order(order_id, asset, context, data):
    """ this function is called on every trade update. """
    orders = context.orders
    if order_id in orders:
        order = orders[order_id]
        if order.pending > 0:
            print_msg(f'order {order_id} is pending')
            return
        context.entry_price[asset] = order.average_price
        on_data(partial(check_exit, asset))
        off_trade(context.order_monitors[asset])
        msg = f'traded order {order_id} for {asset} at '
        msg = msg + f'{context.entry_price[asset]},'
        msg = msg + ' set up exit monitor.'
        print_msg(msg)

def enter_trade(context, data):
    """ this function is called only once at the beginning. """
    if not context.traded:
        px = data.current(context.assets, 'close')
        # for more than one asset, set up a loop and create 
        # the monitoring function using partial from functools
        for asset in context.assets:
            # place a limit order at the last price
            order_id = order_target(asset, 1, px[asset])
            f = partial(check_order, order_id, asset)
            context.order_monitors[asset]=f
            on_trade(f)
            msg = f'placed a new trade {order_id} for {asset},'
            msg = msg + ' and set up order monitor.'
            print_msg(msg)
        context.traded = True

def check_exit(asset, context, data):
    """ this function is called on every data update. """
    px = data.current(asset, 'close')
    move = (px-context.entry_price[asset])/context.entry_price[asset]
    print_msg(f'the move for {asset} is {move}')
    if move > context.take_profit:
        # we hit the take profit target, book profit and terminate
        order_target(asset, 0)
        off_data()
        print_msg(f'booking profit for {asset} at {px} and turn off data monitor.')
        terminate()
    elif move < -context.stop_loss:
        # we hit the stoploss, sqaure off and terminate
        order_target(asset, 0)
        off_data()
        print_msg(f'booking loss for {asset} at {px} and turn off data monitor.')
        terminate()

def initialize(context):
    """ this function is called once at the start of the execution. """
    context.assets = [symbol('AAPL'), symbol('KO')]
    context.take_profit = 0.0005
    context.stop_loss = 0.0005
    context.traded = False
    context.entry_price = {}
    context.order_monitors = {}
    context.data_monitors = {}
    
def handle_data(context, data):
    """ this function is called every minute. """
    enter_trade(context, data)