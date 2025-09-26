from blueshift.api import order_target_percent, symbol
from blueshift.api import schedule_function, date_rules, time_rules
from blueshift.api import set_commission, set_slippage
from blueshift.finance import commission, slippage
from blueshift.library.technicals.indicators import bbands, ema, rsi
        
def advisor_bbands(px):
    upper, mid, lower = bbands(px, 300)
    if upper - lower == 0:
        return 0
    
    last_px = px[-1]
    dist_to_upper = 100*(upper - last_px)/(upper - lower)

    if dist_to_upper > 95:
        return -1
    elif dist_to_upper < 5:
        return 1
    elif dist_to_upper > 40 and dist_to_upper < 60:
        return 0
    else:
        return 999
    
def advisor_rsi(px):
    sig = rsi(px)
    
    if sig > 70:
        return -1
    elif sig < 30:
        return 1
    elif sig > 45 and sig < 55:
        return 0
    else:
        return 999
    
def advisor_ma(px):
    sig1 = ema(px, 5)
    sig2 = ema(px, 20)
    
    if sig1 > sig2:
        return 1
    else:
        return -1
    

def initialize(context):
    context.advisor = advisor_ma
    context.can_trade = False
    context.target_position = {}
    context.signals = {}
        
    set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))
    context.securities = [symbol('RELIANCE', product_type='margin'),symbol('INFY', product_type='margin')]
    
    schedule_function(start_trading, date_rules.everyday(), time_rules.at('09:30'))
    schedule_function(run_strategy, date_rules.everyday(), time_rules.every_nth_minute(5))
    schedule_function(stop_trading, date_rules.everyday(), time_rules.at('15:00'))
    
def run_strategy(context, data):
    if context.can_trade:
        generate_signals(context, data)
        generate_target_position(context, data)
        rebalance(context, data)
    
def rebalance(context, data):
    for security in context.securities:
        if security in context.target_position:
            order_target_percent(
                security, context.target_position[security])
        
def generate_target_position(context, data):
    weight = get_weight(context)

    for security in context.securities:
        if context.signals[security] == 999:
            continue
        elif context.signals[security] > 0.5:
            context.target_position[security] = weight
        elif context.signals[security] < -0.5:
            context.target_position[security] = -weight
        else:
            context.target_position[security] = 0
            
def generate_signals(context, data):
    try:
        price_data = data.history(context.securities, 'close', 375, '1m')
    except:
        return

    for security in context.securities:
        px = price_data.loc[:,security].values
        context.signals[security] = context.advisor(px)
        
def start_trading(context, data):
    context.can_trade = True

def stop_trading(context, data):
    context.can_trade = False
    
def get_weight(context):
    num_secs = len(context.securities)
    return round(1.0/num_secs,2)*2