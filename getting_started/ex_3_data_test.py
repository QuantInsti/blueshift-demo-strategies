"""
    Demo for data fetch using the data object. See 
    https://blueshift.quantinsti.com/api-docs/context.html#data-object
    for more details. Run this for one day using the US-Eqities dataset.
"""

from blueshift.api import symbol

def initialize(context):
    context.universe = [symbol("AAPL"), symbol("MSFT")]

def before_trading_start(context, data):
    print('#'*20, 'current', '#'*20)
    px1 = data.current(context.universe[0], 'close')
    px2 = data.current(context.universe[0], ['open','close'])
    px3 = data.current(context.universe, 'close')
    px4 = data.current(context.universe, ['open','close'])

    print(px1)
    print('-'*40)
    print(px2)
    print('-'*40)
    print(px3)
    print('-'*40)
    print(px4)

    print('#'*20, 'history', '#'*20)
    px1 = data.history(context.universe[0], "close", 3, "1m")
    px2 = data.history(context.universe[0], ['open','close'], 3, "1m")
    px3 = data.history(context.universe, "close", 3, "1m")
    px4 = data.history(context.universe, ["open","close"], 3, "1m")

    print(px1)
    print('-'*40)
    print(px2)
    print('-'*40)
    print(px3)
    print('-'*40)
    print(px4)
    print('-'*40)
    print(px4.xs(context.universe[0]))
    print('-'*40)
    print(px4['close'])

