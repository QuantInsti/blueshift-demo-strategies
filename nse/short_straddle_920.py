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
from blueshift.api import set_stoploss, set_takeprofit
from blueshift.api import schedule_function, date_rules, time_rules

def initialize(context):
    context.strategy_name = 'NIFTY Weeklies Short Straddle'
    context.lotsize = 50
    context.params = {
            'entry':5,
            'exit':30,
            'lots':1,
            'stoploss':0.4,
            'takeprofit':0.4,
            'margin':0.15
            }
    
    try:
        context.params['entry'] = int(context.params['entry'])
        assert context.params['entry'] <= 15
        assert context.params['entry'] >= 5
    except:
        msg = 'entry must be an integer between 5 and 15 (minutes).'
        raise ValueError(msg)
        
    try:
        context.params['exit'] = int(context.params['exit'])
        assert context.params['exit'] <= 60
        assert context.params['exit'] >= 15
    except:
        msg = 'squareoff must be an integer between 15 and 60 (minutes).'
        raise ValueError(msg)
        
    try:
        sl = float(context.params['stoploss'])
        if sl < 0.2 or sl > 0.8:raise ValueError()
    except:
        'stoploss must be a fraction between 0.2 (20%) to 0.80 (80%).'
        raise ValueError(msg)
        
    try:
        tp = float(context.params['takeprofit'])
        if tp < 0.2 or tp > 0.8:raise ValueError()
    except:
        'takeprofit must be a fraction between 0.2 (20%) to 0.80 (80%).'
        raise ValueError(msg)
    
    context.universe = [
                        symbol('NIFTY-W0CE+0'),
                        symbol('NIFTY-W0PE-0'),
                        ]
    entry = context.params['entry']
    exit_ = context.params['exit']
    schedule_function(
            enter, date_rules.every_day(), 
            time_rules.market_open(entry))
    schedule_function(
            close_out, date_rules.every_day(), 
            time_rules.market_close(exit_))
    context.capital_checked = False
    context.traded = False
    context.mock = True
    
def before_trading_start(context, data):
    if not context.capital_checked:
        px = data.current(symbol('NIFTY-I'), 'close')
        lots = context.params['lots']
        required = context.lotsize*px*lots*2*context.params['margin']
        capital = context.portfolio.starting_cash
        if capital < required:
            msg = f'Required capital is {required}, alloted {capital}, '
            msg += f'please add more capital or reduce number of lots.'
            raise ValueError(msg)
        msg = f'Starting strategy {context.strategy_name} '
        msg += f'with parameters {context.params}'
        print(msg)
        context.capital_checked = True
    
    context.entered = set()
    context.traded = False    

def enter(context, data):
    if context.traded:
        return
    
    close_out(context, data)
    size = context.params['lots']*context.lotsize
    for asset in context.universe:
        order_target(asset,-size)
    
    # done for the day
    context.traded = True

def close_out(context, data):
    for oid in context.open_orders:
        cancel_order(oid)
        
    for asset in context.portfolio.positions:
        order_target(asset, 0)
            
def set_targets(context, data):
    if len(context.universe) == len(context.entered):
        return
    
    for asset in context.portfolio.positions:
        if asset in context.entered:
            continue
        set_stoploss(asset, 'PERCENT', context.params['stoploss'])
        set_takeprofit(asset, 'PERCENT', context.params['takeprofit'])
        context.entered.add(asset)
    
def handle_data(context, data):
    if context.mock:
        # for live/ paper try to immideately once started
        context.universe = [symbol('NIFTY-ICE+0'), symbol('NIFTY-IPE+0')]
        enter(context, data)
        
    set_targets(context, data)