
import sqlite3
import pandas as pd
import numpy as np

con = sqlite3.connect('F:/nempy_test_files/historical_mms.db')

price_data_nsw = pd.read_sql_query("select * from DISPATCHPRICE where REGIONID == 'NSW1' and SETTLEMENTDATE >= '2018/12/01 00:00:00' and SETTLEMENTDATE <= '2020/01/01 00:00:00'", con=con)

demand_data_nsw = pd.read_sql_query("select * from DISPATCHREGIONSUM where REGIONID == 'NSW1' and SETTLEMENTDATE >= '2018/12/01 00:00:00' and SETTLEMENTDATE <= '2020/01/01 00:00:00'", con=con)

dispatch_data_tumut = pd.read_sql_query("select * from DISPATCHLOAD where DUID == 'UPPTUMUT' and SETTLEMENTDATE >= '2018/12/01 00:00:00' and SETTLEMENTDATE <= '2020/01/01 00:00:00'", con=con)

data = pd.merge(price_data_nsw, demand_data_nsw, on=['SETTLEMENTDATE'])
data = pd.merge(data, dispatch_data_tumut, on=['SETTLEMENTDATE'])
data = data.sort_values('SETTLEMENTDATE')
data = data.loc[:, ['SETTLEMENTDATE', 'TOTALDEMAND', 'ROP']]
data.to_csv('nsw_2019.csv')

# xs = np.linspace(data['TOTALDEMAND'].min(), data['TOTALDEMAND'].max(), 10)
#
# sample = []
#
# for i in range(0, len(xs) - 1):
#     bin = data[(xs[i] <= data['TOTALDEMAND']) & (data['TOTALDEMAND'] < xs[i+1])]
#     sample.append(bin.sample(100, replace=True))
#
# sample = pd.concat(sample)
# sample.to_csv('sample.csv')