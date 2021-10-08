"""
    Title: Utility functions
    Description: This is a collection of some utility functions
    Style tags: Not applicable
    Asset class: All
    Dataset: Not applicable
"""
import math
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from sklearn.ensemble import RandomForestRegressor
    
PLATFORM_ENGINE = None

try:
    from blueshift.api import get_open_orders, cancel_order, order_target, square_off, get_datetime
    PLATFORM_ENGINE = 'blueshift'
except ImportError:
    from zipline.api import get_open_orders, cancel_order, order_target, get_datetime
    PLATFORM_ENGINE = 'zipline'
  
def cancel_open_orders(context, asset=None):
    """
        Cancel open order for a given asset, or all assets.
        
        Args:
            `context (obj)`: Algo context object.
            
            `asset (obj)`: Asset to square off, or `None` for all.
            
        Returns:
            None.
    """
    def blueshift_f(context, asset): 
        open_orders = get_open_orders() 
        for oo in open_orders: 
            if not asset:
                cancel_order(oo)
            elif oo.asset == asset:
                cancel_order(oo)
             
    def zipline_f(context, asset):
        open_orders = get_open_orders()
        if not open_orders:
            return
        
        if asset:
            orders = open_orders.get(asset, None)
            if orders:
                for oo in orders:
                    cancel_order(oo.id)
            return
        
        for key in open_orders:
            orders = open_orders[key]
            for oo in orders:
                cancel_order(oo.id)
            
    if PLATFORM_ENGINE == 'blueshift':
        return blueshift_f(context, asset)
    else:
        return zipline_f(context, asset)
    
def cancel_all_open_orders(context):
    return cancel_open_orders(context)

def squareoff(context, asset=None):
    """
        Square off assets. If `asset` is `None`, square of all assets.
        Else the specified asset.
        
        Args:
            `context (obj)`: Algo context object.
            
            `asset (obj)`: Asset to square off, or `None` for all.
            
        Returns:
            None.
    """
    if asset:
        if PLATFORM_ENGINE == 'blueshift':
            square_off(asset)
        else:
            order_target(asset, 0)
        return
    
    cancel_open_orders(context)
    
    if PLATFORM_ENGINE == 'blueshift':
        square_off()
    else:
        positions = context.portfolio.positions
        for asset in positions:
            order_target(asset, 0)
            
def _get_entry_side_price(position):
    if PLATFORM_ENGINE == 'zipline':
        side = 'long' if position.amount > 0 else 'short'
        entry_price = position.cost_basis
        current_price = position.last_sale_price
    else:
        side = 'long' if position.quantity > 0 else 'short'
        if side == 'long':
            entry_price = position.buy_price
        else:
            entry_price = position.sell_price
            
        current_price = position.last_price
        
    return side, entry_price, current_price
    
def handle_stop_loss(context, data, asset, method, target):
    """
        Monitor stop loss activities for all assets or a given 
        asset, based on `method` and a `target`. Use this function 
        in `handle_data` to monitor and trigger this activity.
        
        Note:
            This function monitors the current positions for 
            assets. If the pnl for a position has hit the target, it
            will place a square-off order. Supported methods for 
            targets are 
            
            - `PRICE`: The `target` is the price to trigger square-off.
            - `MOVE`: The `target` is the difference between entry 
                and current price to trigger square-off.
            - `PERCENT`: The `target` is the percent move (in points).
            
        If `asset` is None, the function is applied for all assets,  
        else only the given asset.
        
        Args:
            `context (obj)`: The algo context object.
            
            `data (obj)`: The algo data object.
            
            `asset (obj)`: Asset to track (`None` for all).
            
            `method (str)`: Method of take profit.
            
            `target (number)`: Target for this action.
            
        Returns:
            None
    """
    target = abs(target)
    
    positions = context.portfolio.positions
    if not positions:
        return
    
    def _apply_target(asset, side, entry, current):
        hit = False
        if method == 'PRICE':
            hit = current < target if side == 'long' else current > target
        elif method == 'MOVE':
            move = current - entry
            hit = -move > target if side == 'long' else move > target
        elif method == 'PERCENT':
            move = 100*(current/entry -1)
            hit = -move > target if side == 'long' else move > target
            
        if hit:
            squareoff(context, asset)
    
    if asset is None:
        for asset in positions:
            side, entry, current = _get_entry_side_price(positions[asset])
            _apply_target(asset, side, entry, current)
    else:
        if asset not in positions:
            return
        side, entry, current = _get_entry_side_price(positions[asset])
        _apply_target(asset, side, entry, current)
        

def handle_take_profit(context, data, asset, method, target):
    """
        Monitor take profit activities for all assets or a given 
        asset, based on `method` and a `target`. Use this function 
        in `handle_data` to monitor and trigger this activity.
        
        Note:
            This function monitors the current positions for 
            assets. If the pnl for a position has hit the target, it
            will place a square-off order. Supported methods for 
            targets are 
            
            - `PRICE`: The `target` is the price to trigger square-off.
            - `MOVE`: The `target` is the difference between entry 
                and current price to trigger square-off.
            - `PERCENT`: The `target` is the percent move (in points).
            
        If `asset` is None, the function is applied for all assets,  
        else only the given asset.
        
        Args:
            `context (obj)`: The algo context object.
            
            `data (obj)`: The algo data object.
            
            `asset (obj)`: Asset to track (`None` for all).
            
            `method (str)`: Method of take profit.
            
            `target (number)`: Target for this action.
            
        Returns:
            None
    """
    target = abs(target)
    
    positions = context.portfolio.positions
    if not positions:
        return
    
    def _apply_target(asset, side, entry, current):
        hit = False
        if method == 'PRICE':
            hit = current > target if side == 'long' else current < target
        elif method == 'MOVE':
            move = current - entry
            hit = move > target if side == 'long' else -move > target
        elif method == 'PERCENT':
            move = 100*(current/entry -1)
            hit = move > target if side == 'long' else -move > target
            
        if hit:
            squareoff(context, asset)
            
    if asset is None:
        for asset in positions:
            side, entry, current = _get_entry_side_price(positions[asset])
            _apply_target(asset, side, entry, current)
    else:
        if asset not in positions:
            return
        side, entry, current = _get_entry_side_price(positions[asset])
        _apply_target(asset, side, entry, current)
    
def position_size_function(func, param, size):
    """
        Function to map a signal number to a position size. Use this 
        function for position size, mapped between -1 to +1.
        
        Note:
            Currently, the following functions are implemented.
            
            - `BINARY`: generates either +1 or -1 depending on if the
                `size` is greater or less than `param`.
            - `STEP`: generates +1 if `size` is greater than `param`,
                -1 if `size` is less than `-param` or returns 0. Here
                `param` must positive number.
            - `RELU`: generates +1 or -1 same as above, and interpoaltes
                linearly between these if `size` is between this range.
            - `SIGMOID`: generates a smooth sigmoid output between +1 
                and -1, using `param` as the exponentiation factor.
        
        Args:
            `func (str)`: sizing function.
            
            `param (number)`: parameter to specify position function.
            
            `size (number)`: input size.
            
        Returns:
            Number. Position sizing for the input.
    """
    if func not in ['BINARY','STEP', 'RELU', 'SIGMOID']:
        raise ValueError('function not recognized')
        
    if func == 'BINARY':
        return 1 if size > param else -1
    elif func == 'STEP':
        if size > param:
            return 1
        elif size < -param:
            return -1
        else:
            return 0
    elif func == 'RELU':
        sign = 1 if size > 0 else -1
        size = abs(size)
        if size > param:
            return sign
        else:
            return sign*(size/param)
    elif func == 'SIGMOID':
        sign = 1 if size > 0 else -1
        size = abs(size)
        factor = math.exp(size*param)/(1 + math.exp(size*param))
        return sign*factor

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
                px = prices.xs(security)
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
