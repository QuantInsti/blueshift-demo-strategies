# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 22:40:13 2022

@author: prodi
"""


from blueshift.api import symbol, order_target, set_slippage, get_datetime
from blueshift.api import schedule_function, date_rules, time_rules
from blueshift.finance import slippage

def initialize(context):
    context.universe = [
                        symbol('NIFTY-W0CE+0'),
                        symbol('NIFTY-W0PE-0'),
                        ]
    schedule_function(
            enter, date_rules.every_day(), time_rules.market_open(5))
    schedule_function(
            close_out, date_rules.every_day(), time_rules.market_close(30))
    set_slippage(slippage.NoSlippage())

def enter(context, data):
    close_out(context, data)
    px = data.current(context.universe, 'close')

    for asset in context.universe:
        order_target(asset,-50)

def close_out(context, data):
    positions = context.portfolio.positions
    if positions:
        for asset in positions:
            order_target(asset, 0)

def handle_data(context, data):
    pos = context.portfolio.positions
    if pos:
        for asset in pos:
            entry = pos[asset].sell_price
            current_price = data.current(pos[asset].asset,'close')
            if current_price > 1.4*entry or current_price < 0.6*entry:
                order_target(asset, 0)