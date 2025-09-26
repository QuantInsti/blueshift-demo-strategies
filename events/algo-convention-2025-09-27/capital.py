from blueshift.api import order_target_percent, symbol, get_context
from blueshift.api import schedule_function, date_rules, time_rules
from blueshift.api import set_commission, set_slippage, record
from blueshift.api import fund_transfer, add_strategy
from blueshift.protocol import Strategy
from blueshift.finance import commission, slippage
from blueshift.library.technicals.indicators import bbands, ema, rsi

import numpy as np

def fixed_weight(perfs):
    return

def equal_weight(perfs):
    return 1

def growth(perfs):
    cum_rets = perfs['algo_cum_returns'].iloc[-1]
    return np.log(1 + max(0, cum_rets))

def kelly(perfs):
    rets = perfs['algo_returns'].mean()
    var = perfs['algo_returns'].var()
    return max(0, rets/var)

def sharpe(perfs):
    n = len(perfs)
    cum_rets = 1+ perfs['algo_cum_returns'].iloc[-1]
    cum_rets = cum_rets**(1/n) - 1
    vol = perfs['algo_returns'].std()
    return max(0, cum_rets/vol)

class CapitalAllocator:
    def __init__(self, context):
        self.context = context
        self.changes = {}
        self.cash = 0
        self.nu = 0.25
        self.max = 0.90
        self.min = 0.05
        self.perfs_lookback = 60
        self.min_perfs = 20
        self.incremental = True
        self.weights = {}
        
    def initialize(self):
        init_cap = self.context.portfolio.starting_cash
        transferred = fund_transfer(-init_cap)
        self.cash -= round(transferred, 2)
        
        contexts = {s.name:get_context(s.name) for s in self.context.strategies}
        n = len(contexts)
        
        self.changes = {k:round(init_cap/n) for k in contexts}
        self.weights = {k:1/n for k in contexts}
        
        for s in self.context.strategies:
            self.allocate(s)
        
    def compute(self):
        contexts = {s.name:get_context(s.name) for s in self.context.strategies}
        contexts = {k:v for k,v in contexts.items() if v is not None}
            
        self.changes = self.compute_allocation(contexts)
        
    def compute_allocation(self, contexts):
        total_value = sum(
                [contexts[s].portfolio.portfolio_value for s in contexts])
        capital = total_value + self.cash
        record(capital=capital, cash=self.cash)
        
        n = len(contexts)
        metrics = {}
        weighted_metrics = 0
        
        # compute the metrics
        for k in contexts:
            metric = self.compute_metrics(contexts[k])
            if metric is None:
                continue
            
            metrics[k] = metric
            weighted_metrics += metric*self.weights[k]
            
        if not metrics:
            # no change in allocation
            record(**{k:self.weights[k] for k in contexts})
            return {k:0 for k in contexts}
        
        if weighted_metrics==0:
            # if all values are 0, set equal allocation
            metrics = {k:1/n for k in contexts}
            weighted_metrics = 1/n
                
        # apply the exponential updates
        total_update = 0
        updates = {}
        for k in metrics:
            if self.incremental:
                updates[k] = np.exp(self.nu*metrics[k]/weighted_metrics)*self.weights[k]
            else:
                updates[k] = metrics[k]
            total_update += updates[k]
            
        # computes the new weights applying max and min allocation
        weights = {}
        total_weights = 0
        for k in metrics:
            weights[k] = max(self.min, min(updates[k]/total_update, self.max))
            total_weights += weights[k]
        
        # compute the change in capital allocation to action
        changes = {}
        for k in metrics:
            self.weights[k] = weights[k]/total_weights
            changes[k] = self.weights[k]*capital - contexts[k].portfolio.portfolio_value
        
        record(**{k:self.weights[k] for k in contexts})
        return changes
    
    def compute_metrics(self, sub_context):
        perfs =  sub_context.pnls.iloc[-self.perfs_lookback:]
        if len(perfs) < self.min_perfs:
            return
        
        return growth(perfs)
        
    def allocate(self, strategy):
        capital_to_add = self.changes.get(strategy.name, 0)
        if capital_to_add == 0:
            return
        
        strategy.capital_change = capital_to_add

class AdvisorStrategy(Strategy):
    def __init__(self, name, advisor, allocator):
        self.advisor = advisor
        self.allocator = allocator
        self.can_trade = False
        self.target_position = {}
        self.signals = {}
        self.capital_change = 0
        super().__init__(name, 0)

    def initialize(self, context):
        set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
        set_slippage(slippage.FixedSlippage(0.00))
        self.securities = [symbol('RELIANCE', product_type='margin'),symbol('INFY', product_type='margin')]
        
        schedule_function(self.release_capital, date_rules.everyday(), time_rules.at('09:15'))
        schedule_function(self.start_trading, date_rules.everyday(), time_rules.at('09:30'))
        schedule_function(self.run_strategy, date_rules.everyday(), time_rules.every_nth_minute(5))
        schedule_function(self.stop_trading, date_rules.everyday(), time_rules.at('15:00'))
        
        transferred = fund_transfer(self.capital_change)
        self.allocator.cash -= round(transferred, 2)
        self.capital_change = 0
        
    def before_trading_start(self, context, data):
        self.allocator.allocate(self)
        self.can_trade = False
        self.target_position = {}
        self.signals = {}
        
        if self.capital_change > 0:
            transferred = fund_transfer(self.capital_change)
            self.allocator.cash -= round(transferred, 2)
            self.capital_change = 0
            
    def release_capital(self, context, data):
        if self.capital_change < 0:
            weight = self.get_weight()
            for asset in context.portfolio.positions:
                pos = context.portfolio.positions[asset]
                sign = 1 if pos.quantity >= 0 else -1
                net = context.portfolio.portfolio_value
                frac = (net + self.capital_change)*weight*sign/net
                order_target_percent(asset, frac)

    def run_strategy(self, context, data):
        if self.can_trade:
            self.generate_signals(context, data)
            self.generate_target_position(context, data)
            self.rebalance(context, data)
        
    def rebalance(self, context,data):
        for security in self.securities:
            if security in self.target_position:
                order_target_percent(
                    security, self.target_position[security])
            
    def generate_target_position(self, context, data):
        weight = self.get_weight()
    
        for security in self.securities:
            if self.signals[security] == 999:
                continue
            elif self.signals[security] > 0.5:
                self.target_position[security] = weight
            elif self.signals[security] < -0.5:
                self.target_position[security] = -weight
            else:
                self.target_position[security] = 0
                
    def generate_signals(self, context, data):
        try:
            price_data = data.history(self.securities, 'close', 375, '1m')
        except:
            return
    
        for security in self.securities:
            px = price_data.loc[:,security].values
            self.signals[security] = self.advisor(px)
            
    def start_trading(self, context, data):
        self.can_trade = True
        
        if self.capital_change != 0:
            transferred = fund_transfer(self.capital_change)
            self.allocator.cash -= round(transferred, 2)
        
        self.capital_change = 0
    
    def stop_trading(self, context, data):
        self.can_trade = False
        
    def get_weight(self):
        num_secs = len(self.securities)
        return round(1.0/num_secs,2)*2
        
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
    context.allocator = CapitalAllocator(context)
    context.strategies = [
            AdvisorStrategy('bbands', advisor_bbands, context.allocator),
            AdvisorStrategy('rsi', advisor_rsi, context.allocator),
            AdvisorStrategy('xma', advisor_ma, context.allocator),
            ]
    
    for s in context.strategies:
        add_strategy(s)
        
    context.allocator.initialize()
    set_commission(commission.PerShare(cost=0.0, min_trade_cost=0.0))
    set_slippage(slippage.FixedSlippage(0.00))
    
def before_trading_start(context, data):
    context.allocator.compute()
