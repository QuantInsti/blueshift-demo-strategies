"""
    test the call order of different API functions
"""
from blueshift.api import (get_datetime, schedule_function, 
                        date_rules, time_rules)

def initialize(context):
    print("{}:inside initialize".format(get_datetime()))

    schedule_function(rebalance, date_rule=date_rules.month_start(),
                        time_rule=time_rules.market_open())

    context.frequency = 120
    context.loop_count = 0

def rebalance(context, data):
    print("{}:inside rebalance".format(get_datetime()))

def before_trading_start(context, data):
    context.loop_count = 0
    print("{}:inside before_trading_start".format(get_datetime()))

def handle_data(context, data):
    if time_to_run(context):
        print("{}:inside handle_data".format(get_datetime()))

def time_to_run(context):
    flag = True if context.loop_count % context.frequency == 45 else False
    context.loop_count = context.loop_count + 1
    return flag
