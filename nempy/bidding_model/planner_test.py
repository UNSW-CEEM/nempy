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

p = planner.DispatchPlanner(1, price_data.copy(), -25.0, 25.0)
p.add_storage_size(100.0, 50.0)
p.optimise()
dispatch = p.get_dispatch()

fig, axs = plt.subplots(1, 1, tight_layout=True)
axs.scatter(dispatch['interval'], dispatch['dispatch'], color='blue', label='training data')
ax2=axs.twinx()
ax2.scatter(price_data['interval'], price_data[0], color='red', label='prediction')
axs.legend()
fig.tight_layout()

fig2, axs2 = plt.subplots(1, 1, tight_layout=True)
axs2.scatter(dispatch['interval'], dispatch['storage'], color='blue', label='training data')
ax22=axs.twinx()
ax22.scatter(price_data['interval'], price_data[0], color='red', label='prediction')
axs2.legend()
fig.tight_layout()

plt.show()