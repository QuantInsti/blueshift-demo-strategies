# Copyright 2025 QuantInsti Quantitative Learnings Pvt Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Created on Thu Nov 14 17:10:02 2024

@author: QuantInsti
"""

import numpy as np
import datetime

from blueshift.api import symbol, order_with_retry, get_datetime, log_info
from blueshift.api import schedule_function, date_rules, time_rules
from blueshift.api import schedule_at, square_off, wait_for_trade
from blueshift.api import finish, schedule_once, schedule_later, set_cooloff_period
from blueshift.api import set_slippage
from blueshift.finance import slippage
from blueshift.errors import SymbolNotFound
    
def reset(context):
    context.atmf = None
    context.next_roll = None
    context.opts = set()
    context.hedge = None
    context.can_rebalance = False
        
def entry_trade(context, data, series, msg=None):
    if msg:
        log_info(msg)
        
    reset(context)
    dt = get_datetime()
    
    basket = {}
    
    try:
        for delta in ['40','30', '25']:
            basket = {
                    symbol(f'{context.underlying}-{series}CE+{delta}D', dt=dt):0,
                    symbol(f'{context.underlying}-{series}CE+50D', dt=dt):-1,
                    symbol(f'{context.underlying}-{series}PE+50D', dt=dt):-1,
                    symbol(f'{context.underlying}-{series}PE-{delta}D', dt=dt):0,
                    }
            if len(basket) == 4:
                break
    except SymbolNotFound as e:
        log_info(f'Failed to resolve delta symbol:{str(e)}.')
        
    if len(basket) != 4:
        msg = f'{dt}: failed to enter trade: could not calculate basket.'
        exit_trade(context, data, msg)
        return
    
    oids = []
    try:
        for opt, weight in basket.items():
            if weight == 0:
                continue
            ids = order_with_retry(opt, weight*opt.mult*context.lots)
            oids.extend(ids)
            context.opts.add(opt)
            
        wait_for_trade(oids)
    except Exception as e:
        msg = f'{dt}: failed to enter trade: {str(e)}'
        exit_trade(context, data, msg)
    else:
        context.atmf = list(basket.keys())[2].strike # pick the 50D put
        context.can_rebalance = True
        expiry = list(basket.keys())[0].expiry_date.date()
        context.next_roll = expiry - datetime.timedelta(days=context.roll)
        context.hedge = symbol(
                f"{context.underlying}{expiry.strftime('%Y%m%d')}FUT")
        
def exit_trade(context, data, msg=None):
    if msg:
        log_info(msg)
    
    try:
        square_off(ioc=True)
    except Exception as e:
        msg = f'Failed to square-off positions:{str(e)}.'
        finish(msg)
    else:
        reset(context)
        
def rebalance_delta(context, data):
    if not context.can_rebalance or not context.opts:
        return
    
    delta = delta2 = 0
    for asset in context.portfolio.positions:
        pos = context.portfolio.positions[asset]
        try:
            if asset.is_opt():
                d = data.current(asset, 'delta')
            else:
                d = 1
            assert not np.isnan(d), f"Got NaN value for delta for {asset}."
        except Exception as e:
            msg = f'Failed to compute delta:{str(e)}.'
            log_info(msg)
            return
            #exit_trade(context, data, msg)
        else:
            delta2 += d
            delta += d*pos.quantity
            
    if abs(delta2) > context.delta:
        qty = int(delta/context.hedge.mult)*context.hedge.mult
        if abs(qty) > 0:
            try:
                oids = order_with_retry(context.hedge, -qty)
                wait_for_trade(oids)
            except Exception as e:
                msg = f'Failed to rebalance delta: {str(e)}.'
                exit_trade(context, data, msg)
    
def rollover(context, data):
    def roll_entry(context, data):
        entry_trade(context, data, "II",'Entering new positions for rollover')
        
    if not context.can_rebalance or not context.opts:
        return
    
    context.can_rebalance = False
    exit_trade(context, data, 'Exiting current positions for rollover')
    schedule_later(roll_entry, 5)
    
def rebalance(context, data):
    def rebalance_entry(context, data):
        entry_trade(context, data, "I",'Entering new positions for rebalance')
        
    if not context.can_rebalance or not context.opts:
        return
    
    atm = data.current(context.hedge, 'close')
    
    if abs(atm/context.atmf-1) < context.strike:
        return
    
    context.can_rebalance = False
    exit_trade(context, data, 'Exiting current positions for rollover')
    schedule_later(rebalance_entry, 5)
    
def strategy(context, data):
    if not context.opts:
        entry_trade(context, data, "I",'Initiating new positions')
    else:
        rebalance(context, data)
    
def initialize(context):
    context.underlying = 'NIFTY'
    context.lots = 10
    context.roll = 2
    context.delta = 0.0
    context.strike = 0.05
    context.entry_time = "9:30"
    context.rebalance_time = "14:00"
    context.roll_time = "15:15"
    
    reset(context)
    set_cooloff_period(1)
    set_slippage(slippage.NoSlippage())
    
    schedule_function(strategy, 
                      date_rules.every_day(), 
                      time_rules.at(context.entry_time))
    schedule_function(rebalance, 
                      date_rules.every_day(), 
                      time_rules.at(context.rebalance_time))
    
def before_trading_start(context, data):
    def schedule_rollover(context, data):
        schedule_at(rollover, context.roll_time)
        
    dt = get_datetime()
    if context.opts and context.next_roll and dt.date() == context.next_roll:
        schedule_once(schedule_rollover)
        
def on_data(context, data):
    if not context.opts:
        return
    
    rebalance_delta(context, data)
