'''
    Title: Factor models
    Description: This is a sample strategy to demonstrate the general
        principles of creating a factor basket. This involves identifyinh
        a factor, evaluating the factor metrics for each stocks and then
        sorting the stock universe on that metrics. The final step is to
        choose the bottom and top percentile to go short and long 
        respectively to isolate the factor. Fruther works required to 
        analyze the performance of the factor basket - to ascertain if 
        the factor is 1) significant (in returns exaplanation) 2) stable 
        (over period of time) 3) interpretable (have some economic or
        behavioural motivation/ explanation) 4) dispersed (stocks in the
        universe are ranked in a dispersed manner in the factor metrics)
        and finally 5) orthogonal (regressing with known factors does 
        not make returns explaining power of this factor insignificant)
    Style tags: risk factor
    Asset class: Equities, Futures, ETFs, Currencies and Commodities
    Dataset: NSE Daily or NSE Minute
'''

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA as sklearnPCA
from scipy import stats as scipy_stats

# Zipline
from zipline.api import(    symbol,
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
from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.factors import SimpleMovingAverage
from zipline.errors import NoFurtherDataError


def initialize(context):
    # weights for rebalance
    context.weights = {}

    # set up the factor models
    vol_factor = FactorModel('vol_factor',VolatilityFactor,0.05,False)
    skew_factor = FactorModel('skew_factor',SkewFactor,0.05,True)
    pca_factor = FactorModel('pca_factor',PCAFactor,0.05, False)
    mom_factor = FactorModel('mom_factor',MomentumFactor,0.05,True)
    context.agent = Agent([vol_factor, skew_factor, pca_factor, mom_factor],0.35, method=1)

    # set up pipeline - for efficient computation
    attach_pipeline(factor_pipe(context), name='factor_pipe')
    
    # our rebalabce function
    schedule_function(run_strategy,
                    date_rules.month_start(days_offset=0),
                    time_rules.market_open(hours=2, minutes=30))

def before_trading_start(context, data):
    # update pnl trackers every day
    try:
        prices = data.current(context.pipeline_results.index,'close')
        context.agent.update_pnl_history(prices)
    except:
        pass

def run_strategy(context, data):
    # core strategy steps
    context.pipeline_results = run_active_pipe(context).dropna()
    context.agent.update_weights()
    context.agent.compute_weights(context, context.pipeline_results, data)
    rebalance(context, data)

def rebalance(context,data):
    # rebalance function
    for security in context.weights:
        order_target_percent(security, context.weights[security])      

def analyze(context, perf):
    print([asset for asset in context.portfolio.positions])

###################### factor and agent classes ######################
class FactorModel():
    '''
        Factor model class to handle factor basket creation
    '''
    def __init__(self, name, factor_class, percentile=0.05, high_is_long=True):
        self.name = name
        self.factor = factor_class
        self.percentile = percentile
        self.high_is_long = high_is_long
        self.last_px = {}
        self.current_px = {}
        self.last_weights = {}
        self.current_weights = {}
        self.perf = 100.0

    def compute_weights(self,loadings):
        # function to compute factor basket weights
        n = int(len(loadings)*self.percentile)
        if n < 5:
            raise ValueError("number of instruments is too low")
        weight = 0.5/n
        loadings = loadings.sort_values()
        bottom_names = loadings.index[:n]
        top_names = loadings.index[-n:]
        if self.high_is_long:
            long_names = dict((key, weight) for key in top_names)
            short_names = dict((key, -weight) for key in bottom_names)
        else:
            long_names = dict((key, -weight) for key in top_names)
            short_names = dict((key, weight) for key in bottom_names)
        self.last_weights = self.current_weights
        self.current_weights = long_names
        self.current_weights.update(short_names)

    def update_performance(self, prices):
        # update own performance
        self.last_px = dict((k,v) for k,v in self.current_px.iteritems())
        for key in self.current_weights:
            self.current_px[key] = prices[key]

        for key in self.last_weights:
            if self.last_weights[key] <> 0:
                px_change = self.current_px[key] / self.last_px[key] - 1
                if np.isnan(px_change):
                    px_change = 0
                self.perf = self.perf*(1 + self.last_weights[key]*px_change)

class Agent():
    '''
        Agent class to manage multiple factor models
    '''
    def __init__(self,models, learning_rate = 0.2, method=0, lookback=60):
        try:
            self.models = models
            self.n_models = len(self.models)
        except:
            raise ValueError("models must be a list of FactorModel objects")
        
        self.learning_rate = learning_rate
        self.lookback = lookback
        self.method = method
        self.perfs = pd.DataFrame()
        self.weights = pd.DataFrame()
        self.current_weights = {}
        self.initial_weights()

    def initial_weights(self):
        # function to allocate capitals between factor models
        self.current_weights = dict((m.name,1.0/self.n_models) for m in self.models)

    def weighing_function(self):
        # function to allocate capitals between factor models
        #weights = dict((m.name,1.0/self.n_models) for m in self.models)
        weights = dict(zip([m.name for m in self.models],[0,0,1,0]))
        return weights

    def compute_weights(self, context, results, data):
        # computed weighted positions
        context.last_weights = dict(context.weights)
        
        context.weights = {}
        #prices = data.current(results.index,'close')
        for m in self.models:
            w = self.current_weights[m.name]
            m.compute_weights(results[m.name])
            for security in m.current_weights:
                context.weights[security] = context.weights.get(security,0) + m.current_weights[security]*w

        for k in context.last_weights:
            if k not in context.weights and \
                    context.last_weights[k] != 0:
                context.weights[k] = 0

    def update_weights(self):
        # update weights based on the weighing functions
        dt = get_datetime()
        self.current_weights = self.weighing_function()
        weight = pd.DataFrame(self.current_weights, index=[dt])

        if self.weights.empty:
            self.weights = weight
        else:
            self.weights = self.weights.append(weight)

    def update_pnl_history(self, prices):
        # keep pnl history
        dt = get_datetime()
        perfs = dict((m.name,0.0) for m in self.models)
        for m in self.models:
            m.update_performance(prices)
            perfs[m.name] = m.perf
        perfs = pd.DataFrame(perfs, index=[dt])
        if self.perfs.empty:
            self.perfs = perfs
        else:
            self.perfs = self.perfs.append(perfs)
####################### pipeline functions ############################
def factor_pipe(context):
    '''
        function to set up a pipeline to retrieve all active syms. 
        We can add filters here as well.
    '''
    pipe = Pipeline()

    sma_20 = SimpleMovingAverage(inputs=[EquityPricing.close],
                                 window_length=20)
    # Pick the top 50% of stocks ranked by dollar volume
    dollar_volume = AvgDailyDollarVolumeTraded(window_length=252)
    high_dollar_volume = dollar_volume.percentile_between(50, 100)
    # Remove penny stocks
    no_penny_stocks = sma_20 > 1
    filtered_assets = high_dollar_volume & no_penny_stocks
    pipe.set_screen(filtered_assets)

    pipe.add(sma_20, 'sma_20')
    for m in context.agent.models:
        pipe.add(m.factor(inputs=[EquityPricing.close],
                                 window_length=252), m.name)

    return pipe

def run_active_pipe(context):
    return pipeline_output('factor_pipe')

##################### pipeline factors ###############################
class PCAFactor(CustomFactor):
    inputs = [EquityPricing.close]
    window_length = 252
    def compute(self,today,assets,out,close_price):
        out[:] = run_pca(close_price,3,1)

class VolatilityFactor(CustomFactor):
    inputs = [EquityPricing.close]
    window_length = 252
    def compute(self,today,assets,out,close_price):
        out[:] = compute_vols(close_price)

class SkewFactor(CustomFactor):
    inputs = [EquityPricing.close]
    window_length = 252
    def compute(self,today,assets,out,close_price):
        out[:] = compute_skew(close_price)

class MomentumFactor(CustomFactor):
    inputs = [EquityPricing.close]
    window_length = 252
    def compute(self,today,assets,out,close_price):
        out[:] = compute_momentum(close_price,120,1)

class AvgDailyDollarVolumeTraded(CustomFactor):
    inputs = [EquityPricing.close, EquityPricing.volume]
    def compute(self,today,assets,out,close_price,volume):
        dollar_volume = close_price * volume
        out[:] = np.mean(dollar_volume, axis=0)
##################### utility functions ##############################
def run_pca(prices, n_factors, factor_pos):
    '''
        the chosen factor is the factor loading of first PCA compoenent
    '''
    X = np.diff(prices,axis=0)/prices[:-1]
    idx=np.where(np.any(np.isnan(X),axis=0)==True)
    X[:,idx] = 0
    X_std = StandardScaler().fit_transform(X)
    sklearn_pca = sklearnPCA(n_components=n_factors)
    Y_sklearn = sklearn_pca.fit_transform(X_std)
    return sklearn_pca.components_[0]

def compute_vols(prices):
    '''
        the chosen factor is historical realized volatility
    '''
    annualization_factor = np.sqrt(252)
    rets = np.diff(prices,axis=0)/prices[:-1]
    idx=np.where(np.any(np.isnan(rets),axis=0)==True)
    rets[:,idx] = 0
    return np.std(rets, axis=0)*annualization_factor

def compute_momentum(prices, long_n, short_n):
    '''
        the chosen factor is momentum
    '''
    vols = compute_vols(prices)
    vols[np.where(vols==0)] = 1E15
    px1 = prices[-short_n,]
    px2 = prices[-long_n,]
    return (px1-px2)/px2/vols

def compute_skew(prices):
    '''
        the chosen factor is skewness of returns
    '''
    rets = np.diff(prices,axis=0)/prices[:-1]
    idx=np.where(np.any(np.isnan(rets),axis=0)==True)
    rets[:,idx] = 0
    return scipy_stats.skew(rets, axis=0)
    
