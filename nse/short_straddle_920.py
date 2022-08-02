"""
    Title: NIFTY Weeklies Short Straddle
    Description: This strategy sells a straddle on the NSE Nifty weeklies
        at the open, with a given stoploss and take-profit and then 
        squares off near end of the day.
    Style tags: Volatility Premium
    Asset class: Equity Options
    Dataset: NSE
"""
from blueshift.api import symbol, order_target, set_slippage
from blueshift.api import set_stoploss, set_takeprofit,
from blueshift.api import schedule_function, date_rules, time_rules
from blueshift.finance import slippage

def initialize(context):
    context.params = {
            'open':5,
            'squareoff':30,
            'lots':1,
            'stoploss':0.4,
            'takeprofit':0.4
            }
    
    try:
        context.params['open'] = int(context.params['open'])
        assert context.params['lookback'] <= 15
        assert context.params['lookback'] >= 5
    except:
        msg = 'open must be an integer between 5 and 15 (minutes).'
        raise ValueError(msg)
        
    try:
        context.params['squareoff'] = int(context.params['squareoff'])
        assert context.params['squareoff'] <= 15
        assert context.params['squareoff'] >= 5
    except:
        msg = 'squareoff must be an integer between 30 and 60 (minutes).'
        raise ValueError(msg)
        
    try:
        sl = float(context.params['stoploss'])
        if sl < 0.2 or sl > 0.8:raise ValueError()
    except:
        'stoploss must be a fraction between 0.2 (20%) to 0.90 (90%).'
        raise ValueError(msg)
        
    try:
        tp = float(context.params['takeprofit'])
        if tp < 0.2 or tp > 0.8:raise ValueError()
    except:
        'takeprofit must be a fraction between 0.2 (20%) to 0.90 (90%).'
        raise ValueError(msg)
    
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
    for asset in context.universe:
        order_target(asset,-50)
        set_stoploss(asset, 'PERCENT', context.params['stoploss'], 
                trailing=False, on_stoploss=on_exit)

def close_out(context, data):
    positions = context.portfolio.positions
    if positions:
        for asset in positions:
            order_target(asset, 0)
            
def set_targets(context, data):
    pass