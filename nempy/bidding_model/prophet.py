import pandas as pd
from fbprophet import Prophet
import matplotlib.pyplot as plt

price = pd.read_csv('price.csv').loc[:, ['SETTLEMENTDATE', 'RRP']]
demand = pd.read_csv('demand.csv').loc[:, ['SETTLEMENTDATE', 'TOTALDEMAND']]
data = pd.merge(price, demand, on='SETTLEMENTDATE')
data = data[data['SETTLEMENTDATE'] > '2018/01/01 00:00:00']
data = data.rename(columns={'SETTLEMENTDATE': 'ds', 'RRP': 'y'})
train_data = data[data['ds'] < '2019/11/01 00:00:00']
m = Prophet()
m.add_regressor('TOTALDEMAND', mode='multiplicative')
m.fit(train_data)
forecast = m.predict(data)
fig1 = m.plot(forecast)
plt.show()
