'''
    Title: Demo Strategy for querying the data object
    Description: This is a demo strategy to show how the APIs on the data objects can be
        used for current date-time, current price data bar as well as historical data.
    Asset class: All
    Dataset: All (example shows NSE Minute data-set)
'''
# Zipline APIs
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
    context.securities = [symbol('NIFTY-I'), symbol('BANKNIFTY-I')]

def handle_data(context, data):
    '''
        A function to defines things to do at every bar
    '''
    # current simulation date-time
    print('current simulation time {}'.format(data.current_dt))

    # current simulation bar
    current_data = data.current(context.securities[0],'close')  # returns float
    current_data = data.current(context.securities,'close')  # returns series
    current_data = data.current(context.securities[0],['open','close'])  # returns series
    current_data = data.current(context.securities,['open','high','low','close','volume']) # returns dataframe
    print(type(current_data)) # print the return type
    print(current_data) # print the current bar
    print(current_data[context.securities[0]])  # subset on securities and print
    print(current_data['close'])    # subset on field and prnt
    print(current_data.loc[context.securities[0],'close']) # subset on both

    # historical simulation bars
    historical_data = data.history(context.securities[0],'close',3,'1m') # series
    historical_data = data.history(context.securities[0],['open','close'],3,'1m') # dataframe
    historical_data = data.history(context.securities,'close',3,'1m') # dataframe
    historical_data = data.history(context.securities,['open','close'],3,'1m') # panel
    print(type(historical_data))    # print data type
    print(historical_data)  # print data
    
    # sub set dataframe
    print(historical_data[context.securities[0]])   # subset dataframe on security
    
    # subset panel data along the securities axis (minor axis)
    print(historical_data.minor_xs(context.securities[0]))  # subset panel on securities
    print(historical_data.minor_xs(context.securities[0])['close']) # subset panel on securities and then field
    
    # subset panel data along the field axis (item axis)
    print(historical_data['close']) # subset panel on field
    print(historical_data['close'][context.securities[0]])  # subset panel on field and then securities
    
    # converting series data to numpy array (useful for talib functions)
    px_values = historical_data['close'][context.securities[0]].values
    print(type(px_values))
    print(px_values.shape)
    print(px_values)
