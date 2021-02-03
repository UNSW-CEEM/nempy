import pandas as pd
from nempy.bidding_model import planner
import matplotlib.pyplot as plt

from datetime import datetime

from nempy.bidding_model.regional_demand import get_regional_demand
from nempy.bidding_model.interconnector_limits import get_interconnector_limits
from nempy.bidding_model.fleet_dispatch import get_fleet_dispatch
from nempy.bidding_model.region_tech_capacities import get_tech_operating_capacities
from nempy.bidding_model.constraint_mw import get_constrained_mw
from nempy.bidding_model.regional_price import get_regional_prices


from nempy.bidding_model.planner import Forecaster

demand = get_regional_demand('2019/01/01 00:00:00', '2020/01/01 00:00:00')
prices = get_regional_prices('2019/01/01 00:00:00', '2020/01/01 00:00:00')
prices = prices.loc[:, ['SETTLEMENTDATE', 'nsw-energy']]
data = pd.merge(demand, prices, on='SETTLEMENTDATE')
tech = get_tech_operating_capacities('2019/01/01 00:00:00', '2020/01/01 00:00:00')
data = pd.merge(data, tech, on='SETTLEMENTDATE')
#constraint = get_constrained_mw('2019/01/01 00:00:00', '2020/01/01 00:00:00')
#constraint.to_csv('constraint_mw.csv')
constraint = pd.read_csv('constraint_mw.csv')
constraint['SETTLEMENTDATE'] = pd.to_datetime(constraint['SETTLEMENTDATE'], format='%Y/%m/%d %H:%M:%S')
data = pd.merge(data, constraint, on='SETTLEMENTDATE')

data['hour'] = data['SETTLEMENTDATE'].dt.hour
data['weekday'] = data['SETTLEMENTDATE'].dt.dayofweek
data['month'] = data['SETTLEMENTDATE'].dt.month

for col in data.columns:
    if col != 'SETTLEMENTDATE':
        data[col] = pd.to_numeric(data[col])

forward_data = data[data['SETTLEMENTDATE'] < datetime.strptime('2020/01/01 00:00:00', '%Y/%m/%d %H:%M:%S')]
forward_data = \
    forward_data[forward_data['SETTLEMENTDATE'] >=
                 datetime.strptime('2019/12/01 00:00:00', '%Y/%m/%d %H:%M:%S')].reset_index(drop=True)

data = data.rename(columns={'SETTLEMENTDATE': 'interval'})
data['interval'] = pd.to_datetime(data['interval'])
data = data[data['interval'].dt.minute.isin([0, 30])]
data = data.reset_index(drop=True)
data['interval'] = data.index

forward_data = forward_data.rename(columns={'SETTLEMENTDATE': 'interval'})
forward_data['interval'] = pd.to_datetime(forward_data['interval'])
forward_data = forward_data[forward_data['interval'].dt.minute.isin([0, 30])]
forward_data = forward_data.reset_index(drop=True)
forward_data['interval'] = forward_data.index

forcaster = Forecaster()
forcaster.train(data, train_sample_fraction=0.01, target_col='nsw-energy')
forcast = forcaster.base_forecast(forward_data)

fig, axs = plt.subplots(1, 1, tight_layout=True)
axs.scatter(forcast['interval'], forcast['Y'], color='blue', label='forecast')
axs.scatter(forward_data['interval'], forward_data['nsw-energy'], color='red', label='actual')
axs.legend()
fig.tight_layout()

plt.show()