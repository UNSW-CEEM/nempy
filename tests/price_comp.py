import pandas as pd


benchmark = pd.read_csv('1000_interval_benchmark.csv')
outputs = pd.read_csv('latest_1000_interval_run.csv')
comp = pd.merge(outputs, benchmark, on=['time', 'region', 'service'])
comp['diff'] = (comp['error_x'].abs() - comp['error_y'].abs())
comp = comp.sort_values('diff', ascending=False)
x=1