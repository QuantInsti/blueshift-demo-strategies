'''
    Title: Demo strategy for Algo states
    Description: A demo strategy to explain how to access algo states like portfolio 
        details and account statistics
    Asset class: All
    Dataset: All (example shows NSE daily data-set)
'''
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

def initialize(context):
    '''
        A function to define things to do at the start of the strategy
    '''
    # universe selection
    context.securities = [symbol('ACC'), symbol('MARUTI')]

def handle_data(context, data):
    '''
        A function to define things to do at every bar
    '''
    # current simulation date-time
    print('{} {}'.format(data.current_dt.date(), 30*'#'))

    # accessing portfolio details
    portfolio_value = context.portfolio.portfolio_value
    cash = context.portfolio.cash
    positions = context.portfolio.positions
    print('portfolio_value {}, cash {}'.format(portfolio_value, cash))

    for p, pos_data in positions.iteritems():
        print('Symbol {}, cost basis {}'.format(p.symbol, pos_data.cost_basis))

    # accessing account details
    print('leverage {}'.format(context.account.leverage))
    print('net leverage {}'.format(context.account.net_leverage))
    print('available funds {}'.format(context.account.available_funds))
    print('total positions exposure {}'.format(context.account.total_positions_exposure))

    # ordering function
    num_secs = len(context.securities)
    for security in context.securities:
        order_target_percent(security, 1.0/num_secs)
