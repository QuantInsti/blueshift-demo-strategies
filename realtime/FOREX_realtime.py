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

from blueshift.api import (symbol, order_target, get_datetime, terminate,
                           on_data, on_trade, off_data, off_trade)

def print_msg(msg):
    msg = f'{get_datetime()}:' + msg
    print(msg)

def check_order(context, data, order_id):
    """ this function is called on every trade update. """
    if context.asset in context.portfolio.positions:
        print(context.orders)
        order = context.orders[str(order_id)]
        if order.pending > 0:
            print_msg(f'order {order_id} is pending')
            return
        # the order_id is executed, create a function to check for exit
        context.entry_price = order.average_price
        f = lambda context, data:check_exit(context, data, order.asset)
        # schedule the exit func for every data update
        on_data(f)
        # and turn off order monitoring
        off_trade()
        print_msg(f'traded order {order_id}, set up exit monitor.')

def enter_trade(context, data):
    """ this function is called only once at the beginning. """
    if not context.traded:
        px = data.current(context.asset, 'close')
        # place a limit order at the last price
        order_id = order_target(context.asset, 1000, px)
        ## NOTE: do not use lambda for more than one asset inside a loop
        ## (or comprehension), you will only retain the last value of 
        ## closure variable. Use partial from functools. See other 
        ## samples under this folder.
        f = lambda context, data: check_order(context, data, order_id)
        on_trade(f)
        context.traded = True
        msg = f'placed a new trade {order_id} at {px} and set up order monitor.'
        print_msg(msg)

def check_exit(context, data, asset):
    """ this function is called on every data update. """
    px = data.current(asset, 'close')
    move = (px-context.entry_price)/context.entry_price
    
    if move > context.take_profit:
        # we hit the take profit target, book profit and terminate
        order_target(asset, 0)
        off_data()
        print_msg(f'booking profit at {px} and turn off data monitor.')
        terminate()
    elif move < -context.stop_loss:
        # we hit the stoploss, sqaure off and terminate
        order_target(asset, 0)
        off_data()
        print_msg(f'booking loss at {px} and turn off data monitor.')
        terminate()

def initialize(context):
    """ this function is called once at the start of the execution. """
    context.asset = symbol('EUR/USD')
    context.take_profit = 0.005
    context.stop_loss = 0.005
    context.traded = False
    context.entry_price = None
    context.exit_price = None
    
def handle_data(context, data):
    """ this function is called every minute. """
    enter_trade(context, data)