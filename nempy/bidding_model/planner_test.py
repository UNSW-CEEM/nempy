import pandas as pd
from nempy.bidding_model import planner
import matplotlib.pyplot as plt

price_data = pd.read_csv('price_2019_01.csv')
price_data = price_data[price_data['SETTLEMENTDATE'] < '2019/01/04 00:00:00']
price_data = price_data.loc[:, ['RRP']]
price_data['interval'] = price_data.index
price_data.columns = [0, 'interval']
price_data[20] = price_data[0]
price_data[40] = price_data[0]
price_data[60] = price_data[0]
price_data[80] = price_data[0]
price_data[100] = price_data[0]
price_data[-20] = price_data[0]
price_data[-40] = price_data[0]
price_data[-60] = price_data[0]
price_data[-80] = price_data[0]
price_data[-100] = price_data[0]

p = planner.DispatchPlanner(5.0)
p.add_market_node('nsw', price_data.copy())
p.add_unit('stor', 'nsw')
p.add_market_to_unit_flow('stor', 100.0)
p.add_unit_to_market_flow('stor', 100.0)
p.add_storage('stor', mwh=100.0, initial_mwh=50.0, output_capacity=100.0, output_efficiency=0.9,
              input_capacity=100.0, input_efficiency=0.9)
p.optimise()
dispatch = p.get_dispatch()

fig, axs = plt.subplots(1, 1, tight_layout=True)
axs.scatter(dispatch['interval'], dispatch['dispatch'], color='blue', label='dispatch')
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