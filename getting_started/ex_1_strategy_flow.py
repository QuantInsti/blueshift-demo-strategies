"""
    Demo for strategy lifecycle event callbacks. See
    https://blueshift.quantinsti.com/api-docs/events.html for 
    details. Run this for a couple of days starting with the first 
    date of the current month for the any dataset.
"""

from blueshift.api import (get_datetime, schedule_function, 
                        date_rules, time_rules, schedule_later,
                        schedule_once)

def initialize(context):
    print("{}:inside initialize".format(get_datetime()))

    schedule_function(rebalance, date_rule=date_rules.month_start(),
                        time_rule=time_rules.market_open(minutes=30))

    schedule_function(rebalance2, date_rule=date_rules.every_day(),
                        time_rule=time_rules.every_nth_minute(120))

    schedule_once(rebalance3)
    schedule_later(rebalance3, 1)

def before_trading_start(context, data):
    print("{}:inside before_trading_start".format(get_datetime()))

def rebalance(context, data):
    print("{}:inside rebalance".format(get_datetime()))

def rebalance2(context, data):
    print("{}:inside rebalance2".format(get_datetime()))

def rebalance3(context, data):
    print("{}:inside rebalance3".format(get_datetime()))
    schedule_later(rebalance4, 5)

def rebalance4(context, data):
    print("{}:inside rebalance4".format(get_datetime()))
    
