"""
    Demo for order placement. See 
    https://blueshift.quantinsti.com/api-docs/api.html#order-placing-apis
    for more details. Run it for a single day for this demo using the 
    US-Equities dataset.
"""

import pandas as pd

from blueshift.api import (
    symbol, order_target_percent, order_percent, 
    order_target_value, order_value, order_target, 
    order, schedule_function, date_rules, time_rules,
    get_datetime)


def initialize(context):
    context.asset1 = symbol('META')
    context.asset2 = symbol('AAPL')
    context.freq = 60
    schedule_function(trade, date_rules.every_day(), time_rules.
        every_nth_minute(context.freq))

def trade(context, data):
    px = data.current(
        [context.asset1, context.asset2], ['close','volume'])
    
    oid1 = order(context.asset1, 1, limit_price=px.close[context.asset1])
    oid2 = order_target(context.asset2, 1, limit_price=px.close[context.asset2])

    if oid1:
        price = px.close[context.asset1]
        volume = px.volume[context.asset1]
        print(
            f'{get_datetime()}: {context.asset1}@{price}/{volume} sent order ID {oid1}')

    if oid2:
        price = px.close[context.asset2]
        volume = px.volume[context.asset2]
        print(
            f'{get_datetime()}: {context.asset2}@{price}/{volume} sent order ID {oid2}')

# def analyze(context, perf):
#     # see https://blueshift.quantinsti.com/api-docs/objects.html#orders
#     txns_list = context.blotter.transactions
#     txns_df = []
    
#     for dt in txns_list:
#         txns = txns_list[dt]
#         for txn in txns:
#             txns_df.append(txn.to_dict())

#     txns_df = pd.DataFrame(txns_df)[
#         ['oid','timestamp','exchange_timestamp','quantity','average_price',]]
    
#     for idx, row in txns_df.iterrows():
#         msg = f'{row.oid}:placed@{row.timestamp}, '
#         msg += f'traded@{row.exchange_timestamp}, '
#         msg += f'for {row.quantity}@{row.average_price}'
#         print(msg)











