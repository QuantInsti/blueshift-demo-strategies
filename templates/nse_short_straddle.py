"""
    Title: NIFTY Weeklies Short Straddle
    Description: This strategy sells a straddle on the NSE Nifty weeklies
        at the open, with a given stoploss and take-profit and then 
        squares off near end of the day.
    Style tags: Volatility Premium
    Asset class: Equity Options
    Dataset: NSE
    Risk: High
    Minimum Capital: 500,000
"""
from blueshift.api import symbol, order_target, cancel_order
from blueshift.api import set_stoploss, set_takeprofit, schedule_once
from blueshift.api import schedule_function, date_rules, time_rules

def initialize(context):
    context.strategy_name = 'NIFTY Weeklies Short Straddle'
    context.params = {
            'lots':1,
            'stoploss':0.4,
            'takeprofit':0.4,
            }
    
    context.universe = [symbol('NIFTY-W0CE+0'),symbol('NIFTY-W0PE-0')]
    
    schedule_function(
            enter, date_rules.every_day(), 
            time_rules.market_open(5))
    schedule_function(
            close_out, date_rules.every_day(), 
            time_rules.market_close(30))
    
    context.traded = False
    context.margin = 0.15
    
def before_trading_start(context, data):
    context.entered = set()
    context.traded = False    

def enter(context, data):
    if context.traded:
        return
    
    close_out(context, data)
    size = context.params['lots']*context.universe[0].mult
    for asset in context.universe:
        order_target(asset,-size)
    
    # done for the day
    context.traded = True
    schedule_once(set_targets)

def close_out(context, data):
    for oid in context.open_orders:
        cancel_order(oid)
        
    for asset in context.portfolio.positions:
        order_target(asset, 0)
            
def set_targets(context, data):
    # ALWAYS set stoploss and takeprofit targets on positions, 
    # not on order assets. See API documentation for more.
    for asset in context.portfolio.positions:
        if asset in context.entered:
            continue
        set_stoploss(asset, 'PERCENT', context.params['stoploss'])
        set_takeprofit(asset, 'PERCENT', context.params['takeprofit'])
        context.entered.add(asset)
        
    if len(context.universe) != len(context.entered):
        # one or more positions not traded yet, try again alter
        schedule_once(set_targets)