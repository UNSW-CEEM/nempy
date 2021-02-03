from nemosis import data_fetch_methods
import pandas as pd
import numpy as np

raw_data_cache = 'nem_data'


def get_constrained_mw(start_time, end_time):
    bid_cols = ['BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4', 'BANDAVAIL5', 'BANDAVAIL6',
                'BANDAVAIL7', 'BANDAVAIL8', 'BANDAVAIL9', 'BANDAVAIL10']


    bid_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'BIDPEROFFER_D', raw_data_cache,
                                                        select_columns=['DUID', 'INTERVAL_DATETIME',
                                                                        'BIDTYPE', 'MAXAVAIL'] + bid_cols)
    bid_data = bid_data[bid_data['BIDTYPE'] == 'ENERGY']

    bid_data['SETTLEMENTDATE'] = bid_data['INTERVAL_DATETIME']
    bid_data = bid_data.loc[:, ['DUID', 'SETTLEMENTDATE', 'MAXAVAIL'] + bid_cols]
    value_columns = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    bid_data.columns = ['DUID', 'SETTLEMENTDATE', 'MAXAVAIL'] + value_columns

    bid_data = pd.melt(bid_data, id_vars=['DUID', 'SETTLEMENTDATE', 'MAXAVAIL'], value_vars=value_columns,
                       var_name='band', value_name='bid')

    dispatch_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHLOAD', raw_data_cache,
                                                             select_columns=['DUID', 'SETTLEMENTDATE', 'TOTALCLEARED'])

    bid_data = pd.merge(bid_data, dispatch_data, on=['DUID', 'SETTLEMENTDATE'])

    bid_data = bid_data.sort_values(['SETTLEMENTDATE', 'DUID', 'band'])

    bid_data['bid'] = pd.to_numeric(bid_data['bid'])
    bid_data['TOTALCLEARED'] = pd.to_numeric(bid_data['TOTALCLEARED'])

    bid_data['cumulative_bid'] = bid_data.groupby(['SETTLEMENTDATE', 'DUID'], as_index=False).bid.cumsum()

    bid_data['MAXAVAIL'] = pd.to_numeric(bid_data['MAXAVAIL'])

    bid_data['cumulative_bid'] = np.where(bid_data['MAXAVAIL'] > bid_data['cumulative_bid'], bid_data['cumulative_bid'],
                                          bid_data['MAXAVAIL'])

    bid_data['undispatched_bid_cumulative'] = bid_data['cumulative_bid'] - bid_data['TOTALCLEARED']

    bid_data['undispatched_bid_cumulative'] = np.where(bid_data['undispatched_bid_cumulative'] < 0.0, 0.0,
                                                       bid_data['undispatched_bid_cumulative'])

    bid_data['undispatched_bid'] = bid_data.groupby('DUID', as_index=False)['undispatched_bid_cumulative'].diff()

    bid_data['undispatched_bid'] = np.where(bid_data['undispatched_bid'].isnull(),
                                            bid_data['undispatched_bid_cumulative'],
                                            bid_data['undispatched_bid'])

    bid_data = bid_data[bid_data['undispatched_bid'] > 0.0]
    bid_data = bid_data[bid_data['bid'] > 0.0]

    bid_data = bid_data.drop(columns=['undispatched_bid_cumulative', 'MAXAVAIL', 'cumulative_bid', 'TOTALCLEARED',
                                      'bid'])

    bid_cols = ['PRICEBAND1', 'PRICEBAND2', 'PRICEBAND3', 'PRICEBAND4', 'PRICEBAND5', 'PRICEBAND6', 'PRICEBAND7',
                'PRICEBAND8', 'PRICEBAND9', 'PRICEBAND10']

    bid_prices = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'BIDDAYOFFER_D', raw_data_cache,
                                                        select_columns=['DUID', 'SETTLEMENTDATE', 'BIDTYPE'] + bid_cols)

    bid_prices = bid_prices[bid_prices['BIDTYPE'] == 'ENERGY']

    bid_prices = bid_prices.loc[:, ['DUID', 'SETTLEMENTDATE'] + bid_cols]
    value_columns = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    bid_prices.columns = ['DUID', 'SETTLEMENTDATE'] + value_columns

    bid_prices = pd.melt(bid_prices, id_vars=['DUID', 'SETTLEMENTDATE'], value_vars=value_columns,
                         var_name='band', value_name='bid_price')

    bid_prices = bid_prices.sort_values('SETTLEMENTDATE')

    bid_data = pd.merge_asof(bid_data, bid_prices, on=['SETTLEMENTDATE'], by=['DUID', 'band'])

    unit_info = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DUDETAILSUMMARY', raw_data_cache,
                                                              select_columns=['START_DATE', 'END_DATE', 'DUID',
                                                                              'REGIONID',
                                                                              'TRANSMISSIONLOSSFACTOR',
                                                                              'DISTRIBUTIONLOSSFACTOR'])

    unit_info = unit_info.sort_values(['START_DATE', 'DUID'])

    dispatch_price = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHPRICE', raw_data_cache,
                                                              select_columns=['SETTLEMENTDATE', 'REGIONID', 'RRP'])

    bid_data = pd.merge_asof(bid_data, unit_info, left_on=['SETTLEMENTDATE'], right_on=['START_DATE'], by=['DUID'])

    bid_data = pd.merge(bid_data, dispatch_price, on=['SETTLEMENTDATE', 'REGIONID'])

    bid_data['TRANSMISSIONLOSSFACTOR'] = pd.to_numeric(bid_data['TRANSMISSIONLOSSFACTOR'])
    bid_data['DISTRIBUTIONLOSSFACTOR'] = pd.to_numeric(bid_data['DISTRIBUTIONLOSSFACTOR'])
    bid_data['RRP'] = pd.to_numeric(bid_data['RRP'])
    bid_data['bid_price'] = pd.to_numeric(bid_data['bid_price'])

    bid_data['bid_price'] = bid_data['bid_price'] / (bid_data['TRANSMISSIONLOSSFACTOR'] *
                                                     bid_data['DISTRIBUTIONLOSSFACTOR'])

    bid_data = bid_data.drop(columns=['TRANSMISSIONLOSSFACTOR', 'DISTRIBUTIONLOSSFACTOR'])

    bid_data = bid_data[bid_data['bid_price'] <= bid_data['RRP']]

    bid_data = bid_data.groupby(['SETTLEMENTDATE', 'REGIONID'], as_index=False).aggregate({'undispatched_bid': 'sum'})

    bid_data.columns = ['SETTLEMENTDATE', 'REGIONID', 'CONSTRAINT_MW']

    bid_data['REGIONID'] = bid_data['REGIONID'] + '-constraint'

    bid_data = bid_data.pivot_table(values='CONSTRAINT_MW', index='SETTLEMENTDATE', columns='REGIONID')

    bid_data = bid_data.reset_index().fillna('0.0')

    return bid_data


if __name__ == "__main__":
    constraint = get_constrained_mw('2019/01/01 00:00:00', '2020/01/01 00:00:00')
    constraint.to_csv('constraint_mw.csv')