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
from blueshift.api import symbol, order, square_off
from blueshift.api import set_stoploss, set_takeprofit, get_datetime
from blueshift.api import schedule_function, date_rules, time_rules

def initialize(context):
    context.params = {
            'lots':1,
            'stoploss':0.4,
            'takeprofit':0.4,
            'start':'09:20',
            'end':'15:00',
            }
    
    start = context.params['start']
    end = context.params['end']
    schedule_function(enter, date_rules.every_day(), time_rules.at(start))
    schedule_function(close_out, date_rules.every_day(), time_rules.at(end))

def enter(context, data):
    dt = get_datetime()
    opts = [symbol('NIFTY-W0CE+0', dt=dt),symbol('NIFTY-W0PE-0', dt=dt)]
    lots = int(context.params['lots'])
    
    for asset in opts:
        qty = lots*asset.mult
        oid = order(asset,-qty)
        if not oid:
            handle_order_failure(context, asset)
        else:
            sl = float(context.params['stoploss'])
            tp = float(context.params['takeprofit'])
            set_stoploss(asset, method='percent', target=sl)
            set_takeprofit(asset, method='percent', target=tp)

def close_out(context, data):
    square_off()

def handle_order_failure(context, asset):
    pass

