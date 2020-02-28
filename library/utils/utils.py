"""
    Title: Utility functions
    Description: This is a collection of some utility functions
    Style tags: Not applicable
    Asset class: All
    Dataset: Not applicable
"""
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from sklearn.ensemble import RandomForestRegressor

__ENGINE__ = None

try:
    from blueshift.api import (get_open_orders, cancel_order, 
                              order_target_percent, get_datetime)
    __ENGINE__ = 'blueshift'
except ImportError:
    from zipline.api import (get_open_orders, cancel_order, 
                             order_target_percent, get_datetime)
    __ENGINE__ = 'zipline'


def cancel_all_open_orders(context):
    """ cancel all open orders. """
    def blueshift_f(context): 
        open_orders = get_open_orders() 
        for oo in open_orders:  
             cancel_order(oo)
             
    def zipline_f(context):
        open_orders = get_open_orders()
        if not open_orders:
            return
        for key in open_orders:
            orders = open_orders[key]
            if not orders:
                continue
            for order in orders:
                cancel_order(order)
            
    if __ENGINE__ == 'blueshift':
        return blueshift_f(context)
    else:
        return zipline_f(context)
         
def square_off(context):
    """ cancel all open orders. """
    cancel_all_open_orders(context)
    
    positions = context.portfolio.positions
    for asset in positions:
        order_target_percent(asset, 0)
    

def hedge_ratio(Y, X):
    """
        Returns the ADF p-value and regression coefficent (without 
        intercept) of regression of y (dependent) against x (explanatory).
        
        Args:
            Y (series or ndarray or list): input y series
            X (series or ndarray or list): input x series
            
        Returns:
            Tuple. p-Value of Augemented Dickey Fuller test on the 
            regression residuals, and the regression coefficient.
    """
    model = sm.OLS(Y, X).fit()
    resids = model.resid
    p_value = adfuller(resids)[1]
    
    return p_value, model.params[0], resids

def z_score(Y, X=None, lookback=None, coeff=None):
    """
        Given two series Y and X, and a lookback, computes the latest 
        z-score of the regression residual (ratio of deviation from 
        the mean and standard deviation of the residuals).
        
        Note:
            X and Y must be of equial length, lookback must be less than
            or equal to the length of these series.
        
        Args:
            Y (series or ndarray or list): input y series
            X (series or ndarray or list): input x series
            lookback (int): lookback for computation.
            coeff (float): regression coefficient.
            
        Returns:
            z-score of the regression residuals.
    """
    if lookback is None:
        lookback = len(Y)
        
    if X is None:
        spread = Y
    else:
        if coeff is None:
            coeff = hedge_ratio(Y, X)
        spread = Y.values - coeff * X.values
    
    deviation =  (spread[-1] - spread[-lookback:].mean())
    return deviation/spread[-lookback:].std()


def estimate_random_forest(df):
    """
        Estimate random forest regression for input DataFrame, assuming
        the last column to be the predicted variable, and everything 
        else are predictors.
        
        Args:
            df (DataFrame): Merged frame of X and Y of training set.
            
        Returns:
            A random forest fitted model based on the input dataframe.
    """
    regr = RandomForestRegressor()
    regr.fit(df[:,:-1], df[:,-1:].ravel())
    return regr
    
def predict_random_forest(regr, df):
    """
        Forecast using a fitted model, assuming the last row in the 
        input DataFrame are the observation to be predicted and the 
        last but one columns are the predictors in the model.
        
        Args:
            regr (object): A model object for prediction
            
            df (DataFrame): 
    """
    pred = regr.predict(df[-1:,:-1])[0]
    return pred

class ExperAdvisor():
    """
        This is a class that implements individual strategies with 
        a given signal functions. This class computes the weight of 
        each asset in the universe based on its signal function, and 
        also maintains the last theoritical pnl.
    """
    def __init__(self, name, signal_fn, universe, params):
        self.n_assets = len(universe)
        self.name = name
        
        self.leverage = 1.0
        if "leverage" in params:
            self.leverage = params["leverage"]
            
        self.buy_threshold = 0.5
        if "buy_threshold" in params:
            self.buy_threshold = params["buy_threshold"]
            
        self.sell_threshold = -0.5
        if "sell_threshold" in params:
            self.sell_threshold = params["sell_threshold"]
        
        self.signal_fn = signal_fn
        
        self.last_px = dict((security,0.0) for security in universe)
        self.current_px = dict((security,0.0) for security in universe)
        self.last_weights = dict((security,0.0) for security in universe)
        self.current_weights = dict((security,0.0) for \
                                    security in universe)
        
        self.perf = 100.0

    def get_price(self, prices, security):
        try:
            self.last_px[security] = self.current_px[security]
            px = prices.loc[:,security].values
            self.current_px[security] = px[-1]
        except:
            try:
                self.last_px[security] = self.current_px[security]
                px = prices.minor_xs(security)
                self.current_px[security] = px['close'].values[-1]
            except:
                return None
        return px

    def compute_signals(self, prices, *args, **kwargs):
        """
            compute signals and weights for each asset in the universe
            for this advisor.
        """
        weight = round(1.0/self.n_assets,2)*self.leverage
        
        for security in self.universe:
            self.last_weights[security] = self.current_weights[security]
            px = self.get_price(prices, security)
            
            if px is None:
                # if we do not get a valid price, get out
                signal = 0
            else:
                signal = self.signal_fn(px, *args, **kwargs)
            
            if signal == 999:
                continue
            elif signal > self.buy_threshold:
                self.current_weights[security] = weight
            elif signal < self.sell_threshold:
                self.current_weights[security] = -weight
            else:
                self.current_weights[security] = 0.0
        
        self.update_performance()

    def update_performance(self):
        """
            compute unlevered latest performance.
        """
        perf = 0
        for key in self.last_weights:
            if self.last_px[key] != 0:
                px_change = self.current_px[key] / self.last_px[key] - 1
                perf = perf + self.last_weights[key]*px_change
            
        self.perf = self.perf*(1+perf/self.n_assets)
        
class PortfolioManager():
    """
        This is the class that implements strategy selection algorithm, 
        including constant re-balance, random-weight or no-regret 
        algorithms.
    """
    def __init__(self,advisors):
        try:
            self.advisors = advisors
            self.n_advisors = len(self.advisors)
        except:
            raise ValueError("advisors must be a list of Advisor objects")
        
        self.advisors_keys = [advisor.name for advisor in self.advisors]
        self.perfs = pd.DataFrame()
        self.weights = pd.DataFrame()
        self.current_weights = {}
        self.initial_weights()

    def compute_weights(self, context, data, lookback, frequency,
                        *args, **kwargs):
        """
            Called to update the securities weights. It calls 
            the expert advisors to update respective signals and 
            combine them according to current weights assigned to 
            each experts
        """
        weights = dict((security,0.0) for security in context.universe)
        
        prices = data.history(context.universe, 
                              ['open','high','low','close'],
                              lookback, frequency)
        
        for advisor in self.advisors:
            w = self.current_weights[advisor.name]
            advisor.compute_signals(prices, *args, **kwargs)
            for security in context.universe:
                weights[security] = weights[security] + \
                                advisor.current_weights[security]*w
        context.weights = weights

    def update_weights(self):
        """
            Called to update the weighing scheme. It can be scheduled 
            to be called at a lower frequency than signal computation.
        """
        dt = get_datetime()
        self.current_weights = self.weighing_function()
        weight = pd.DataFrame(self.current_weights, index=[dt])

        if self.weights.empty:
            self.weights = weight
        else:
            self.weights = self.weights.append(weight)

    def initial_weights(self):
        """ initial weights are set to equal weighing. """
        self.current_weights = dict((key,1/self.n_advisors) for \
                                    key in self.advisors_keys)

    def weighing_function(self):
        """ over-ride this function for the meta strategy. """
        if len(self.perfs) < 20:
            weights = dict((key,1.0/self.n_advisors) for \
                           key in self.advisors_keys)
            return weights
        
        weights = dict((key,1.0/self.n_advisors) for \
                       key in self.advisors_keys)

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
