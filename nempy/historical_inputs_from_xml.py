import xmltodict
import pandas as pd


def get_unit_initial_conditions_dataframe(inputs):
    traders = inputs['NEMSPDCaseFile']['NemSpdInputs']['TraderCollection']['Trader']
    initial_conditions = dict(unit=[], initial_output=[], ramp_up_rate=[], ramp_down_rate=[])
    name_map = dict(initial_output='InitialMW', ramp_up_rate='SCADARampUpRate', ramp_down_rate='SCADARampDnRate')
    for trader in traders:
        initial_conditions['unit'].append(trader['@TraderID'])
        initial_cons = trader['TraderInitialConditionCollection']['TraderInitialCondition']
        for our_name, aemo_name in name_map.items():
            for con in initial_cons:
                if con['@InitialConditionID'] == aemo_name:
                    value = con['@Value']
                    break
                else:
                    value = ''
            initial_conditions[our_name].append(value)
    initial_conditions = pd.DataFrame(initial_conditions)
    return initial_conditions


def get_unit_volume_bids(inputs):
    traders = inputs['NEMSPDCaseFile']['NemSpdInputs']['PeriodCollection']['Period']['TraderPeriodCollection']['TraderPeriod']
    trades_by_unit_and_type = dict(unit=[], service=[])
    name_map = dict(service='@TradeType')
    for trader in traders:
        if type(trader['TradeCollection']['Trade']) != list:
            trades = trader['TradeCollection']
            for _, trade in trades.items():
                trades_by_unit_and_type['unit'].append(trader['@TraderID'])
                for our_name, aemo_name in name_map.items():
                    if aemo_name in trade:
                        value = trade[aemo_name]
                    else:
                        value = ''
                    trades_by_unit_and_type[our_name].append(value)
        else:
            for trade in trader['TradeCollection']['Trade']:
                trades_by_unit_and_type['unit'].append(trader['@TraderID'])
                for our_name, aemo_name in name_map.items():
                    if aemo_name in trade:
                        value = trade[aemo_name]
                    else:
                        value = ''
                    trades_by_unit_and_type[our_name].append(value)
    trades_by_unit_and_type = pd.DataFrame(trades_by_unit_and_type)
    return trades_by_unit_and_type


with open('../tests/test_files/NEMSPDOutputs_2019010913800.loaded') as file:
    inputs = xmltodict.parse(file.read())
    volume_bids = get_unit_volume_bids(inputs)
    know_types = volume_bids[volume_bids['service'].isin(['ENOF', 'L5MI', 'L60S', 'L6SE', 'L5RE', 'R5MI', 'R60S', 'R6SE', 'R5RE'])]
    initial_conditions = get_unit_initial_conditions_dataframe(inputs)
    x=1



