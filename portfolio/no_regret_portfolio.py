'''
    Title: Technical Strategy - No Regret
    Description: Ensemble strategy implementing no-regret allocation scheme with a 
        defined learning rate.
    Style tags: Mean Reversion, Momentum, Risk Factor
    Asset class: Equities, Equity Futures, ETFs and
    Dataset: NSE Minute
'''
import talib as ta
import pandas as pd
import numpy as np
import bisect

from blueshift.finance import commission, slippage
from blueshift.api import(    symbol,
                            get_datetime,
                            order_target_percent,
                            schedule_function,
                            date_rules,
                            time_rules,
                            attach_pipeline,
                            pipeline_output,
                            set_commission,
                            set_slippage,
                            get_open_orders,
                            cancel_order
                       )

def expert_advisor_1(px, params):
    '''
        expert advisor based on Bollinger Band break-out
    '''
    px = px.close.values
    upper, mid, lower = bollinger_band(px,params['BBands_period'])
    ind2 = ema(px, params['SMA_period_short'])
    ind3 = ema(px, params['SMA_period_long'])
    last_px = px[-1]
    dist_to_upper = 100*(upper - last_px)/(upper - lower)

    if dist_to_upper > 95:
        return -1
    elif dist_to_upper < 5:
        return 1
    elif dist_to_upper > 40 and dist_to_upper < 60 and ind2-ind3 < 0:
        return -1
    elif dist_to_upper > 40 and dist_to_upper < 60 and ind2-ind3 > 0:
        return 1
    else:
        return 0

def expert_advisor_2(px, params):
    '''
        expert advisor based on moving average cross-over momentum
    '''
    px = px.close.values
    ind2 = ema(px, params['SMA_period_short'])
    ind3 = ema(px, params['SMA_period_long'])
    
    if ind2/ind3 > 1.0:
        return 1
    elif ind2/ind3 < 1.0:
        return -1
    else:
        return 0

def expert_advisor_3(px, params):
    '''
        expert advisor based on candle stick patterns and Bollinger Bands
    '''
    ind1 = doji(px)
    upper, mid, lower = bollinger_band(px.close.values,params['BBands_period'])
    last_px = px.close.values[-1]
    dist_to_upper = 100*(upper - last_px)/(upper - lower)

    if ind1 > 0 and dist_to_upper < 30:
        return 1
    elif ind1 > 0 and dist_to_upper > 70:
        return -1
    else:
        return 999

def expert_advisor_4(px, params):
    '''
        expert advisor based on Fibonacci support and resistance breakouts
    '''
    lower, upper = fibonacci_support(px.close.values)
    ind2 = adx(px, params['ADX_period'])

    if lower == -1:
        return -1
    elif upper == -1:
        return 1
    elif upper > 0.02 and lower > 0 and upper/lower > 3 and ind2 < 20:
        return -1
    elif lower > 0.02 and upper > 0 and lower/upper > 3 and ind2 < 20:
        return 1
    else:
        return 999

def initialize(context):
    '''
        A function to define things to do at the start of the strategy
    '''
    # universe selection
    context.universe = [symbol('NIFTY-I'),symbol('BANKNIFTY-I')]
    
    # define strategy parameters
    context.params = {'indicator_lookback':375,
                      'indicator_freq':'1m',
                      'buy_signal_threshold':0.5,
                      'sell_signal_threshold':-0.5,
                      'SMA_period_short':15,
                      'SMA_period_long':60,
                      'RSI_period':300,
                      'BBands_period':300,
                      'ADX_period':120,
                      'trade_freq':15,
                      'leverage':1}
    
    # variable to control trading frequency
    context.bar_count = 0

    # variables to track target portfolio
    context.weights = dict((security,0.0) for security in context.universe)

    # set trading cost and slippage to zero
    set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))

    # create the list of experts as well as the agent controlling them
    expert1 = Advisor('bbands_ea',expert_advisor_1, context.universe)
    expert2 = Advisor('maxover_ea',expert_advisor_2, context.universe)
    expert3 = Advisor('rsi_ea',expert_advisor_3, context.universe)
    expert4 = Advisor('sup_res_ea',expert_advisor_4, context.universe)
    context.agent = Agent([expert1, expert2, expert3, expert4],0.35, method=1)

    # schedule agent weights updates
    schedule_function(update_agent_weights, date_rules.week_start(),
        time_rules.market_close())

def before_trading_start(context, data):
    update_agent_perfs(context, data)

def handle_data(context, data):
    '''
        A function to define things to do at every bar
    '''
    context.bar_count = context.bar_count + 1
    if context.bar_count < context.params['trade_freq']:
        return
    
    # time to trade, reset the counter and call the strategy function
    context.bar_count = 0
    run_strategy(context, data)

def run_strategy(context, data):
    '''
        A function to define core strategy steps
    '''
    # we update the weights through the agent which calls each adviosr in turn
    context.agent.compute_weights(context, data)
    # and then simply call the standard rebalance functions
    rebalance(context, data)

def rebalance(context,data):
    '''
        A function to rebalance - all execution logic goes here
    '''
    for security in context.universe:
        order_target_percent(security, context.weights[security])

def analyze(context, perf):
    # let's see what our portfolio looks like at the end of the back-test run
    print(context.portfolio)

def update_agent_weights(context, data):
    '''
        A function to wrap a call to the agent's update weights function. This is required 
        to match the syntax of the underlying schedule_function API signature
    '''
    context.agent.update_weights()

def update_agent_perfs(context, data):
    '''
        A function to wrap a call to the agent's update performance function. This is 
        required to match the syntax of the underlying API signature
    '''
    context.agent.update_pnl_history()

############################ agents and exper advisors classes ###########################
class Agent():
    '''
        This is the class that implements strategy selection algorithm, including
        constant re-balance, random-weight or no-regret algorithms
    '''
    def __init__(self,advisors, learning_rate = 0.2, method=0, lookback=60):
        try:
            self.advisors = advisors
            self.n_advisors = len(self.advisors)
        except:
            raise ValueError("advisors must be a list of Advisor objects")
        
        self.learning_rate = learning_rate
        self.lookback = lookback
        self.method = method
        self.advisors_keys = [advisor.name for advisor in self.advisors]
        self.perfs = pd.DataFrame()
        self.weights = pd.DataFrame()
        self.current_weights = {}
        self.initial_weights()

    def compute_weights(self, context, data):
        '''
            Called to update the securities weights. It calls the expert functions to
            update respective signals and combine them according to current weights 
            assigned to each experts
        '''
        weights = dict((security,0.0) for security in context.universe)
        prices = data.history(context.universe, ['open','high','low','close'],
            context.params['indicator_lookback'],context.params['indicator_freq'])
        for advisor in self.advisors:
            w = self.current_weights[advisor.name]
            advisor.compute_signals(context.universe, context.params, prices)
            for security in context.universe:
                weights[security] = weights[security] + advisor.current_weights[security]*w
        context.weights = weights

    def update_weights(self):
        '''
            Called to update the weighing scheme. It can be scheduled to be called at 
            a lower frequency than signal computation.
        '''
        dt = get_datetime()
        self.current_weights = self.weighing_function()
        weight = pd.DataFrame(self.current_weights, index=[dt])

        if self.weights.empty:
            self.weights = weight
        else:
            self.weights = self.weights.append(weight)

    def initial_weights(self):
        self.current_weights = dict((key,1/self.n_advisors) for key in self.advisors_keys)

    def weighing_function(self):
        if len(self.perfs) < self.lookback:
            weights = dict((key,1.0/self.n_advisors) for key in self.advisors_keys)
            return weights
        
        last_weights = self.current_weights
        perf = dict((key,0.0) for key in self.advisors_keys)
        updates = dict((key,0.0) for key in self.advisors_keys)
        weights = dict((key,0.0) for key in self.advisors_keys)
        total_perf = 0.0
        
        for advisor in self.advisors:
            initial = self.perfs[advisor.name].iloc[0]
            final = self.perfs[advisor.name].iloc[-1]
            ratio = final/initial
            ret = (final/initial)**(1.0/len(self.perfs)) - 1
            vol = self.perfs[advisor.name].pct_change().dropna().std()
            if self.method == 0:
                perf[advisor.name] = round(max(ret/vol, 0.0),4)
            else:
                perf[advisor.name] = round(ratio,4)
            total_perf = total_perf + perf[advisor.name]*last_weights[advisor.name]

        if total_perf < 1E-20:
            return last_weights
        
        total_update = 0
        for advisor in self.advisors:
            updates[advisor.name] = np.exp(self.learning_rate*perf[advisor.name]/total_perf)
            total_update = total_update + updates[advisor.name]

        if total_update < 1E-20:
            return last_weights
        
        for advisor in self.advisors:
            weights[advisor.name] = updates[advisor.name]/total_update
        return weights
    
    def update_pnl_history(self):
        dt = get_datetime()
        perfs = dict((key,0.0) for key in self.advisors_keys)
        for advisor in self.advisors:
            perfs[advisor.name] = advisor.perf
        perfs = pd.DataFrame(perfs, index=[dt])
        if self.perfs.empty:
            self.perfs = perfs
        else:
            self.perfs = self.perfs.append(perfs)

class Advisor():
    '''
        This is the class that implements individual strategies with individual signal
        functions. This class also maintains the updated pnl of the strategy
    '''
    def __init__(self, name, signal_fn, universe):
        self.n_assets = len(universe)
        self.name = name
        self.signal_fn = signal_fn
        self.last_px = dict((security,0.0) for security in universe)
        self.current_px = dict((security,0.0) for security in universe)
        self.last_weights = dict((security,0.0) for security in universe)
        self.current_weights = dict((security,0.0) for security in universe)
        self.perf = 100.0

    def get_price(self, prices, security):
        try:
            self.last_px[security] = self.current_px[security]
            px = prices.loc[:,security].values
            self.current_px[security] = px[-1]
        except:
            try:
                self.last_px[security] = self.current_px[security]
                px = prices.xs(security)
                self.current_px[security] = px['close'].values[-1]
            except:
                raise ValueError('Unknown type of historical price data')
        return(px)

    def compute_signals(self, universe, params, prices):
        num_secs = len(universe)
        weight = round(1.0/num_secs,2)*params['leverage']
        for security in universe:
            self.last_weights[security] = self.current_weights[security]
            px = self.get_price(prices, security)
            signal = self.signal_fn(px, params)
            if signal == 999:
                pass
            elif signal > params['buy_signal_threshold']:
                self.current_weights[security] = weight
            elif signal < params['sell_signal_threshold']:
                self.current_weights[security] = -weight
            else:
                self.current_weights[security] = 0.0
        self.update_performance()

    def update_performance(self):
        for key in self.last_weights:
            if self.last_px[key] != 0:
                px_change = self.current_px[key] / self.last_px[key] - 1
                self.perf = self.perf*(1 + self.last_weights[key]*px_change/self.n_assets)

############################ common technical indicators #################################

def sma(px, lookback):
    sig = ta.SMA(px, timeperiod=lookback)
    return sig[-1]

def ema(px, lookback):
    sig = ta.EMA(px, timeperiod=lookback)
    return sig[-1]

def rsi(px, lookback):
    sig = ta.RSI(px, timeperiod=lookback)
    return sig[-1]

def bollinger_band(px, lookback):
    upper, mid, lower = ta.BBANDS(px, timeperiod=lookback)
    return upper[-1], mid[-1], lower[-1]

def macd(px, lookback):
    macd_val, macdsignal, macdhist = ta.MACD(px)
    return macd_val[-1], macdsignal[-1], macdhist[-1]

def doji(px):
    sig = ta.CDLDOJI(px.open.values, px.high.values, px.low.values, px.close.values)
    return sig[-1]

def adx(px, lookback):
    signal = ta.ADX(px.high.values, px.low.values, px.close.values, timeperiod=lookback)
    return signal[-1]

def fibonacci_support(px):
    def fibonacci_levels(px):
        return [min(px) + l*(max(px) - min(px)) for l in [0,0.236,0.382,0.5,0.618,1]]

    def find_interval(x, val):
        return (-1 if val < x[0] else 99) if val < x[0] or val > x[-1] \
            else  max(bisect.bisect_left(x,val)-1,0)

    last_price = px[-1]
    lower_dist = upper_dist = 0
    sups = fibonacci_levels(px[:-1])
    idx = find_interval(sups, last_price)

    if idx==-1:
        lower_dist = -1
        upper_dist = round(100.0*(sups[0]/last_price-1),2)
    elif idx==99:
        lower_dist = round(100.0*(last_price/sups[-1]-1),2)
        upper_dist = -1
    else:
        lower_dist = round(100.0*(last_price/sups[idx]-1),2)
        upper_dist = round(100.0*(sups[idx+1]/last_price-1),2)

    return lower_dist,upper_dist

