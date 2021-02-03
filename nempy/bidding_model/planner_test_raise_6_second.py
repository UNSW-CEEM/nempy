import pandas as pd
from nempy.bidding_model import planner
import matplotlib.pyplot as plt

price_data = pd.read_csv('forecast.csv')
price_data = price_data.rename(columns={'SETTLEMENTDATE': 'interval'})
price_data['interval'] = pd.to_datetime(price_data['interval'])
price_data = price_data[price_data['interval'].dt.minute.isin([0, 30])]
price_data.columns = [int(col) if col != 'interval' else col for col in price_data.columns]
price_data = price_data.reset_index(drop=True)
price_data['interval'] = price_data.index

raise_data = pd.read_csv('raise_6_second_forecast.csv')
raise_data = raise_data.rename(columns={'SETTLEMENTDATE': 'interval'})
raise_data['interval'] = pd.to_datetime(raise_data['interval'])
raise_data = raise_data[raise_data['interval'].dt.minute.isin([0, 30])]
raise_data.columns = [int(col) if col != 'interval' else col for col in raise_data.columns]
raise_data = raise_data.reset_index(drop=True)
raise_data['interval'] = raise_data.index

p = planner.DispatchPlanner(30.0)
p.add_regional_market('nsw', 'raise_6_second', raise_data.copy())
p.add_regional_market('nsw', 'energy', price_data.copy())
p.add_unit('stor', 'nsw')
p.set_unit_fcas_region('stor', service='raise_6_second', region='nsw')
p.add_market_to_unit_flow('stor', 1000.0)
p.add_unit_to_market_flow('stor', 1000.0)
p.add_storage('stor', mwh=1000.0, initial_mwh=500.0, output_capacity=1000.0, output_efficiency=0.9,
              input_capacity=1000.0, input_efficiency=0.9)
p.add_raise_6_second_service_to_input('stor', 1000.0)
#p.add_raise_6_second_service_to_output('stor', 1000.0)
p.optimise()
dispatch = p.get_dispatch()

fig, axs = plt.subplots(1, 1, tight_layout=True)
axs.scatter(dispatch['interval'], dispatch['dispatch'], color='blue', label='dispatch')
axs.scatter(dispatch['interval'], dispatch['raise'], color='green', label='raise')
ax2=axs.twinx()
ax2.scatter(price_data['interval'], price_data[0], color='red', label='price')
axs.legend()
fig.tight_layout()

fig2, axs2 = plt.subplots(1, 1, tight_layout=True)
axs2.scatter(dispatch['interval'], dispatch['storage'], color='blue', label='storage level')
ax22=axs2.twinx()
ax22.scatter(price_data['interval'], price_data[0], color='red', label='price')
axs2.legend()
fig.tight_layout()

plt.show()