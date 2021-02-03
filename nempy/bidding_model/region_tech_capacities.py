from nemosis import data_fetch_methods
import pandas as pd

raw_data_cache = 'nem_data'


def get_duid_techs():

    cols = ['DUID', 'Region', 'Fuel Source - Descriptor', 'Technology Type - Descriptor']
    tech_data = data_fetch_methods.static_table_xl('', '', 'Generators and Scheduled Loads', raw_data_cache,
                                                   select_columns=cols)

    def tech_classifier(fuel_source, technology_type):
        category = fuel_source
        if technology_type == 'Hydro - Gravity':
            category = 'Hydro'
        elif technology_type == 'Open Cycle Gas turbines (OCGT)':
            category = 'OCGT'
        elif technology_type == 'Combined Cycle Gas Turbine (CCGT)':
            category = 'CCGT'
        elif technology_type == 'Battery':
            category = 'Battery'
        elif technology_type == 'Run of River':
            category = 'Hydro'
        elif technology_type == 'Spark Ignition Reciprocating Engine':
            category = 'Engine'
        elif technology_type == 'Compression Reciprocating Engine':
            category = 'Engine'
        elif technology_type == 'Steam Sub-Critical' and (fuel_source == 'Natural Gas / Fuel Oil' or fuel_source == 'Natural Gas'):
            category = 'Gas Thermal'
        elif technology_type == 'Pump Storage':
            category = 'Hydro'
        return category

    tech_data['TECH'] = tech_data.apply(lambda x: tech_classifier(x['Fuel Source - Descriptor'],
                                                                      x['Technology Type - Descriptor']),
                                            axis=1)

    return tech_data.loc[:, ['DUID', 'Region', 'TECH']]


def get_tech_operating_capacities(start_time, end_time):
    #bid_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'BIDPEROFFER_D', raw_data_cache,
    #                                                    select_columns=['DUID', 'SETTLEMENTDATE', 'INTERVAL_DATETIME',
    #                                                                    'BIDTYPE', 'MAXAVAIL'])
    #bid_data = bid_data[bid_data['BIDTYPE'] == 'ENERGY']
    dispatch_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHLOAD', raw_data_cache,
                                                             select_columns=['DUID', 'SETTLEMENTDATE', 'AVAILABILITY'])
    tech_data = get_duid_techs()

    #bid_data = pd.merge(bid_data, tech_data, on='DUID')
    dispatch_data = pd.merge(dispatch_data, tech_data, on='DUID')

    #bid_data['MAXAVAIL'] = pd.to_numeric(bid_data['MAXAVAIL'])
    dispatch_data['AVAILABILITY'] = pd.to_numeric(dispatch_data['AVAILABILITY'])

    #bid_data = bid_data.groupby(['TECH', 'Region', 'SETTLEMENTDATE'], as_index=False).aggregate({'MAXAVAIL': 'sum'})
    dispatch_data = dispatch_data.groupby(['TECH', 'Region', 'SETTLEMENTDATE'], as_index=False).aggregate({'AVAILABILITY': 'sum'})

    dispatch_data['tech_region'] = dispatch_data['TECH'] + '-' + dispatch_data['Region'] + '-capacity'

    dispatch_data = dispatch_data.pivot_table(values='AVAILABILITY', index='SETTLEMENTDATE', columns='tech_region')

    dispatch_data = dispatch_data.reset_index().fillna('0.0')

    return dispatch_data


if __name__ == "__main__":
    get_tech_operating_capacities('2019/01/01 00:00:00', '2019/01/01 00:30:00')