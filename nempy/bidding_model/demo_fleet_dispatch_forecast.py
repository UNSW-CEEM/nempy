import pandas as pd
from nempy.bidding_model import planner
import matplotlib.pyplot as plt

from nempy.bidding_model.regional_demand import get_regional_demand
from nempy.bidding_model.interconnector_limits import get_interconnector_limits
from nempy.bidding_model.fleet_dispatch import get_fleet_dispatch

from nempy.bidding_model.planner import Forecaster

fleet_dispatch = get_fleet_dispatch('2019/01/01 00:00:00', '2020/01/01 00:00:00', fleet_units=['MUSSELR1'])
demand = get_regional_demand('2019/01/01 00:00:00', '2020/01/01 00:00:00')
inter_limits = get_interconnector_limits('2019/01/01 00:00:00', '2020/01/01 00:00:00')

fleet_dispatch['SETTLEMENTDATE'] = pd.to_datetime(fleet_dispatch['SETTLEMENTDATE'])
data = pd.merge(demand, fleet_dispatch, on='SETTLEMENTDATE')

for col in data.columns:
    if col != 'SETTLEMENTDATE':
        data[col] = pd.to_numeric(data[col])

forward_data = data[data['SETTLEMENTDATE'] < '2020/01/01 00:00:00']
forward_data = forward_data[forward_data['SETTLEMENTDATE'] >= '2019/12/01 00:00:00'].reset_index(drop=True)

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
forcaster.train(data, train_sample_fraction=0.01, target_col='fleet_dispatch')
forcast = forcaster.base_forecast(forward_data)

fig2, axs2 = plt.subplots(1, 1, tight_layout=True)
axs2.scatter(forcast['interval'], forcast['fleet_dispatch'], color='blue', label='forecast')
ax22=axs2.twinx()
ax22.scatter(forward_data['interval'], forward_data['fleet_dispatch'], color='red', label='actual')
axs2.legend()
fig2.tight_layout()

plt.show()