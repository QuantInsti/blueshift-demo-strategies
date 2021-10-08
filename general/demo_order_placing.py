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
                            order_percent,
                            order_target_value,
                            order_value,
                            order_target,
                            order,
                       )

def initialize(context):
    context.asset = symbol('MARUTI')
    context.order_func = order_target
    context.freq = 60
    context.bar_count = 0

def handle_data(context,data):
    if context.bar_count != context.freq:
        context.bar_count = context.bar_count + 1
        return

    context.bar_count = 0
    context.order_func(context.asset, 1)

def analyze(context, perf):
    txns = (perf['transactions'].iloc[-1])
    for txn in txns:
        if txn:
            print('{}: traded {} units at {}'.format(
                txn['dt'], txn['amount'], txn['price']))

