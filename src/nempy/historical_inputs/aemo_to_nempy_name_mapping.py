
name_map = {'TOTALCLEARED': 'energy',
            'RAISEREG': 'raise_reg',
            'LOWERREG': 'lower_reg',
            'RAISE1SEC': 'raise_1s',
            'RAISE6SEC': 'raise_6s',
            'RAISE60SEC': 'raise_60s',
            'RAISE5MIN': 'raise_5min',
            'LOWER1SEC': 'lower_1s',
            'LOWER6SEC': 'lower_6s',
            'LOWER60SEC': 'lower_60s',
            'LOWER5MIN': 'lower_5min',
            'ENERGY': 'energy',
            'DUID': 'unit',
            'BIDTYPE': 'service',
            'MAXAVAIL': 'capacity',
            'UIGF': 'capacity',
            'MinLoadingMW': 'min_loading',
            'CurrentMode': 'current_mode',
            'CurrentModeTime': 'time_in_current_mode',
            'T1': 'mode_one_length',
            'T2': 'mode_two_length',
            'T3': 'mode_three_length',
            'T4': 'mode_four_length',
            'GENERATOR': 'generator',
            'LOAD': 'load',
            'LOSSFACTOR': 'loss_factor',
            'DISPATCHTYPE': 'dispatch_type',
            'CONNECTIONPOINTID': 'connection_point',
            'REGIONID': 'region',
            'INTERCONNECTORID': 'interconnector',
            'LOSSCONSTANT': 'loss_constant',
            'LOSSFLOWCOEFFICIENT': 'flow_coefficient',
            'FROMREGIONLOSSSHARE': 'from_region_loss_share',
            'DEMANDCOEFFICIENT': 'demand_coefficient',
            'LOSSSEGMENT': 'loss_segment',
            'MWBREAKPOINT': 'break_point',
            'RAMPUPRATE': 'ramp_up_rate',
            'RAMPDOWNRATE': 'ramp_down_rate',
            'SCADARAMPUPRATE': 'scada_ramp_up_rate',
            'SCADARAMPDOWNRATE': 'scada_ramp_down_rate',
            'INITIALMW': 'initial_output',
            'DIRECTION': 'dispatch_type'}


def map_aemo_column_names_to_nempy_names(dataframe):
    for name in dataframe.columns:
        if name not in name_map.keys():
            raise ValueError("No mapping for '{}' available.".format(name))
    return dataframe.rename(columns=name_map)


def map_aemo_column_values_to_nempy_name(dataframe, column):
    dataframe[column] = dataframe[column].apply(lambda x: name_map[x])
    return dataframe

