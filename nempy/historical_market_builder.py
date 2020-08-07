import sqlite3
import pandas as pd
import pytest
import numpy as np

from nempy import markets
from nempy.spot_markert_backend import check
from nempy.help_functions import helper_functions as hf
from nempy.historical import historical_spot_market_inputs as hi, historical_interconnector_loss_models as hii


class SpotMarket:
    def __init__(self, inputs_database, inputs, interval):
        self.con = sqlite3.connect(inputs_database)
        self.inputs_manager = hi.DBManager(connection=self.con)
        self.inputs = inputs
        self.interval = interval
        self.services = ['TOTALCLEARED', 'LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC',
                         'LOWERREG', 'RAISEREG']
        self.service_name_mapping = {'TOTALCLEARED': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg',
                                     'RAISE6SEC': 'raise_6s', 'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min',
                                     'LOWER6SEC': 'lower_6s', 'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min',
                                     'ENERGY': 'energy'}
        self.unit_inputs = self.inputs.get_unit_inputs(self.interval)
        unit_info = self.unit_inputs.get_unit_info()
        self.market = markets.SpotMarket(market_regions=['QLD1', 'NSW1', 'VIC1', 'SA1', 'TAS1'], unit_info=unit_info)

    def add_unit_bids_to_market(self):
        volume_bids, price_bids = self.unit_inputs.get_processed_bids()
        self.market.set_unit_volume_bids(volume_bids)
        self.market.set_unit_price_bids(price_bids)

    def set_unit_limit_constraints(self):
        unit_bid_limit = self.unit_inputs.get_unit_bid_availability()
        self.market.set_unit_bid_capacity_constraints(unit_bid_limit)
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['unit_capacity']
        self.market.make_constraints_elastic('unit_bid_capacity', violation_cost=cost)
        unit_ugif_limit = self.unit_inputs.get_unit_uigf_limits()
        self.market.set_unconstrained_intermitent_generation_forecast_constraint(unit_ugif_limit)
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['ugif']
        self.market.make_constraints_elastic('uigf_capacity', violation_cost=cost)

    def set_ramp_rate_limits(self):
        ramp_rates = self.unit_inputs.get_ramp_rates_used_for_energy_dispatch()
        self.market.set_unit_ramp_up_constraints(
            ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_up_rate']])
        self.market.set_unit_ramp_down_constraints(
            ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_down_rate']])
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['ramp_rate']
        self.market.make_constraints_elastic('ramp_up', violation_cost=cost)
        self.market.make_constraints_elastic('ramp_down', violation_cost=cost)

    def set_fast_start_constraints(self):
        self.market.dispatch(price_market_constraints=False)
        dispatch = self.market.get_unit_dispatch()
        dispatch = dispatch[dispatch['service'] == 'energy']
        fast_start_profiles = self.unit_inputs.get_fast_start_profiles(dispatch)
        fast_start_profiles = fast_start_profiles.loc[:, ['unit', 'end_mode', 'time_in_end_mode', 'mode_two_length',
                                                          'mode_four_length', 'min_loading']]
        self.market.set_fast_start_constraints(fast_start_profiles)
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['fast_start']
        try:
            self.market.make_constraints_elastic('fast_start', cost)
        except check.ModelBuildError:
            pass

    def measured_violation_equals_historical_violation(self, historical_name, nempy_constraints):
        measured = 0.0
        for name in nempy_constraints:
            measured += self.market.get_elastic_constraints_violation_degree(name)
        historical = self.unit_inputs.xml_inputs.get_non_intervention_violations()[historical_name]
        if historical > 0.0 or measured > 0.0:
            x=1
        return measured == pytest.approx(historical, abs=0.1)

    def set_unit_fcas_constraints(self):
        self.unit_inputs.add_fcas_trapezium_constraints()

        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['fcas_max_avail']
        fcas_availability = self.unit_inputs.get_fcas_max_availability()
        self.market.set_fcas_max_availability(fcas_availability)
        self.market.make_constraints_elastic('fcas_max_availability', cost)

        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['fcas_profile']

        regulation_trapeziums = self.unit_inputs.get_fcas_regulation_trapeziums()
        self.market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)
        self.market.make_constraints_elastic('energy_and_regulation_capacity', cost)

        scada_ramp_down_rates = self.unit_inputs.get_scada_ramp_down_rates()
        lower_reg_units = self.unit_inputs.get_lower_reg_units_with_scada_ramp_rates()
        scada_ramp_down_rates = scada_ramp_down_rates[scada_ramp_down_rates['unit'].isin(lower_reg_units['unit'])]
        self.market.set_joint_ramping_constraints_lower_reg(scada_ramp_down_rates)
        self.market.make_constraints_elastic('joint_ramping_lower_reg', cost)

        scada_ramp_up_rates = self.unit_inputs.get_scada_ramp_up_rates()
        raise_reg_units = self.unit_inputs.get_raise_reg_units_with_scada_ramp_rates()
        scada_ramp_up_rates = scada_ramp_up_rates[scada_ramp_up_rates['unit'].isin(raise_reg_units['unit'])]
        self.market.set_joint_ramping_constraints_raise_reg(scada_ramp_up_rates)
        self.market.make_constraints_elastic('joint_ramping_raise_reg', cost)

        contingency_trapeziums = self.unit_inputs.get_contingency_services()
        self.market.set_joint_capacity_constraints(contingency_trapeziums)
        self.market.make_constraints_elastic('joint_capacity', cost)

    def set_region_demand_constraints(self):
        DISPATCHREGIONSUM = self.inputs_manager.DISPATCHREGIONSUM.get_data(self.interval)
        regional_demand = hi.format_regional_demand(DISPATCHREGIONSUM)
        self.market.set_demand_constraints(regional_demand.loc[:, ['region', 'demand']])
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['regional_demand']
        self.market.make_constraints_elastic('demand', cost)

    def add_interconnectors_to_market(self):
        interconnector_inputs = self.inputs.get_interconnector_inputs(self.interval)
        interconnector_inputs.add_loss_model()
        interconnectors = interconnector_inputs.get_interconnector_definitions()
        market_interconnectors = interconnector_inputs.get_market_interconnector_links()
        loss_functions, interpolation_break_points = interconnector_inputs.get_interconnector_loss_model()

        interconnectors['link'] = interconnectors['interconnector']
        interconnectors['from_region_loss_factor'] = 1.0
        interconnectors['to_region_loss_factor'] = 1.0
        interconnectors['generic_constraint_factor'] = 1

        interconnectors = pd.concat([interconnectors, market_interconnectors])
        interconnectors['generic_constraint_factor'] = interconnectors['generic_constraint_factor'].astype(np.int64)

        interpolation_break_points = pd.merge(interconnectors.loc[:, ['interconnector', 'link', 'generic_constraint_factor']],
                                              interpolation_break_points, on='interconnector')
        interpolation_break_points['break_point'] = interpolation_break_points['break_point'] * interpolation_break_points['generic_constraint_factor']
        interpolation_break_points['loss_segment'] = interpolation_break_points['loss_segment'] * \
                                                    interpolation_break_points['generic_constraint_factor']
        interpolation_break_points = interpolation_break_points.drop('generic_constraint_factor', axis=1)

        loss_functions = pd.merge(interconnectors.loc[:, ['interconnector', 'link', 'generic_constraint_factor']],
                                  loss_functions, on='interconnector')

        def loss_function_adjuster(loss_function, generic_constraint_factor):
            def wrapper(flow):
                return loss_function(flow * generic_constraint_factor)
            return wrapper

        loss_functions['loss_function'] = \
            loss_functions.apply(lambda x: loss_function_adjuster(x['loss_function'], x['generic_constraint_factor']), axis=1)

        loss_functions = loss_functions.drop('generic_constraint_factor', axis=1)

        self.market.set_interconnectors(interconnectors)
        self.market.set_interconnector_losses(loss_functions, interpolation_break_points)

    def add_generic_constraints(self):
        DISPATCHCONSTRAINT = self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)
        DUDETAILSUMMARY = self.inputs_manager.DUDETAILSUMMARY.get_data(self.interval)
        GENCONDATA = self.inputs_manager.GENCONDATA.get_data(self.interval)
        SPDINTERCONNECTORCONSTRAINT = self.inputs_manager.SPDINTERCONNECTORCONSTRAINT.get_data(self.interval)
        SPDREGIONCONSTRAINT = self.inputs_manager.SPDREGIONCONSTRAINT.get_data(self.interval)
        SPDCONNECTIONPOINTCONSTRAINT = self.inputs_manager.SPDCONNECTIONPOINTCONSTRAINT.get_data(self.interval)

        generic_rhs = hi.format_generic_constraints_rhs_and_type(DISPATCHCONSTRAINT, GENCONDATA)
        unit_generic_lhs = hi.format_generic_unit_lhs(SPDCONNECTIONPOINTCONSTRAINT, DUDETAILSUMMARY)
        region_generic_lhs = hi.format_generic_region_lhs(SPDREGIONCONSTRAINT)

        interconnector_generic_lhs = hi.format_generic_interconnector_lhs(SPDINTERCONNECTORCONSTRAINT)
        bass_link, interconnector_generic_lhs = self._split_out_bass_link(interconnector_generic_lhs)
        bass_link_forward_direction = hii.create_forward_flow_interconnectors(bass_link)
        bass_link_reverse_direction = hii.create_reverse_flow_interconnectors(bass_link)
        interconnector_generic_lhs = pd.concat([interconnector_generic_lhs, bass_link_forward_direction,
                                                bass_link_reverse_direction])

        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['voll']

        self.market.set_generic_constraints(generic_rhs)
        self.market.make_constraints_elastic('generic', violation_cost=0.0)
        self.market.link_units_to_generic_constraints(unit_generic_lhs)
        self.market.link_regions_to_generic_constraints(region_generic_lhs)
        self.market.link_interconnectors_to_generic_constraints(interconnector_generic_lhs)

    def add_generic_constraints_fcas_requirements(self):

        generic_rhs = self.unit_inputs.xml_inputs.get_constraint_rhs()
        generic_type = self.unit_inputs.xml_inputs.get_constraint_type()
        generic_rhs = pd.merge(generic_rhs, generic_type.loc[:, ['set', 'type']], on='set')
        type_map = {'LE': '<=', 'EQ': '=', 'GE': '>='}
        generic_rhs['type'] = generic_rhs['type'].apply(lambda x: type_map[x])

        bid_type_map = dict(ENOF='energy', LDOF='energy', L5RE='lower_reg', R5RE='raise_reg', R5MI='raise_5min',
                            L5MI='lower_5min', R60S='raise_60s', L60S='lower_60s', R6SE='raise_6s',
                            L6SE='lower_6s')

        unit_generic_lhs = self.unit_inputs.xml_inputs.get_constraint_unit_lhs()
        unit_generic_lhs['service'] = unit_generic_lhs['service'].apply(lambda x: bid_type_map[x])
        region_generic_lhs = self.unit_inputs.xml_inputs.get_constraint_region_lhs()
        region_generic_lhs['service'] = region_generic_lhs['service'].apply(lambda x: bid_type_map[x])
        interconnector_generic_lhs = self.unit_inputs.xml_inputs.get_constraint_interconnector_lhs()

        violation_cost = generic_type.loc[:, ['set', 'cost']]

        #pos_cons = generic_rhs[generic_rhs['rhs'] > 0.0]
        fcas_requirements = pd.merge(region_generic_lhs, generic_rhs, on='set')
        fcas_requirements = fcas_requirements.loc[:, ['set', 'service', 'region', 'type', 'rhs']]
        fcas_requirements.columns = ['set', 'service', 'region', 'type', 'volume']
        self.market.set_fcas_requirements_constraints(fcas_requirements)
        self.market.make_constraints_elastic('fcas', violation_cost=violation_cost)

        generic_rhs = generic_rhs[~generic_rhs['set'].isin(list(fcas_requirements['set']))]
        region_generic_lhs = region_generic_lhs[~region_generic_lhs['set'].isin(list(fcas_requirements['set']))]
        self.market.set_generic_constraints(generic_rhs)
        self.market.make_constraints_elastic('generic', violation_cost=violation_cost)
        self.market.link_units_to_generic_constraints(unit_generic_lhs)
        self.market.link_interconnectors_to_generic_constraints(interconnector_generic_lhs)
        self.market.link_regions_to_generic_constraints(region_generic_lhs)

    def set_unit_dispatch_to_historical_values(self, wiggle_room=0.001):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)

        bounds = DISPATCHLOAD.loc[:, ['DUID'] + self.services]
        bounds.columns = ['unit'] + self.services

        bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=self.services, type_name='service',
                                  value_name='dispatched')

        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        decision_variables = self.market._decision_variables['bids'].copy()

        decision_variables = pd.merge(decision_variables, bounds, on=['unit', 'service'])

        decision_variables_first_bid = decision_variables.groupby(['unit', 'service'], as_index=False).first()

        def last_bids(df):
            return df.iloc[1:]

        decision_variables_remaining_bids = \
            decision_variables.groupby(['unit', 'service'], as_index=False).apply(last_bids)

        decision_variables_first_bid['lower_bound'] = decision_variables_first_bid['dispatched'] - wiggle_room
        decision_variables_first_bid['upper_bound'] = decision_variables_first_bid['dispatched'] + wiggle_room
        decision_variables_first_bid['lower_bound'] = np.where(decision_variables_first_bid['lower_bound'] < 0.0, 0.0,
                                                               decision_variables_first_bid['lower_bound'])
        decision_variables_first_bid['upper_bound'] = np.where(decision_variables_first_bid['upper_bound'] < 0.0, 0.0,
                                                               decision_variables_first_bid['upper_bound'])
        decision_variables_remaining_bids['lower_bound'] = 0.0
        decision_variables_remaining_bids['upper_bound'] = 0.0

        decision_variables = pd.concat([decision_variables_first_bid, decision_variables_remaining_bids])

        self.market._decision_variables['bids'] = decision_variables

    def all_dispatch_units_and_service_have_decision_variables(self, wiggle_room=0.001):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)

        bounds = DISPATCHLOAD.loc[:, ['DUID'] + self.services]
        bounds.columns = ['unit'] + self.services

        bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=self.services, type_name='service',
                                  value_name='dispatched')

        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        bounds = bounds[bounds['dispatched'] > 0.001]

        decision_variables = self.market._decision_variables['bids'].copy()

        decision_variables = decision_variables.groupby(['unit', 'service'], as_index=False).first()

        decision_variables = pd.merge(bounds, decision_variables, how='left', on=['unit', 'service'])

        decision_variables['not_missing'] = ~decision_variables['variable_id'].isna()

        decision_variables = decision_variables.sort_values('not_missing')

        return decision_variables['not_missing'].all()


    def set_interconnector_flow_to_historical_values(self, wiggle_room=0.1):
        # Historical interconnector dispatch
        DISPATCHINTERCONNECTORRES = self.inputs_manager.DISPATCHINTERCONNECTORRES.get_data(self.interval)
        interconnector_flow = DISPATCHINTERCONNECTORRES.loc[:, ['INTERCONNECTORID', 'MWFLOW']]
        interconnector_flow.columns = ['interconnector', 'flow']
        interconnector_flow['link'] = interconnector_flow['interconnector']
        interconnector_flow['link'] = np.where(interconnector_flow['interconnector'] == 'T-V-MNSP1',
            np.where(interconnector_flow['flow'] >= 0.0, 'BLNKTAS', 'BLNKVIC'), interconnector_flow['link'])

        flow_variables = self.market._decision_variables['interconnectors']
        flow_variables = pd.merge(flow_variables, interconnector_flow, 'left', on=['interconnector', 'link'])
        flow_variables = flow_variables.fillna(0.0)
        flow_variables['flow'] = np.where(flow_variables['link'] != flow_variables['interconnector'],
                                          flow_variables['flow'].abs(), flow_variables['flow'])
        flow_variables['lower_bound'] = flow_variables['flow'] - wiggle_room
        flow_variables['upper_bound'] = flow_variables['flow'] + wiggle_room
        flow_variables = flow_variables.drop(['flow'], axis=1)
        self.market._decision_variables['interconnectors'] = flow_variables

    @staticmethod
    def _split_out_bass_link(interconnectors):
        bass_link = interconnectors[interconnectors['interconnector'] == 'T-V-MNSP1']
        interconnectors = interconnectors[interconnectors['interconnector'] != 'T-V-MNSP1']
        return bass_link, interconnectors

    def dispatch(self, calc_prices=True):
        if 'OCD' in self.unit_inputs.xml_inputs.get_file_name():
            self.market.dispatch(price_market_constraints=calc_prices, allow_over_constrained_dispatch_re_run=True,
                                 energy_market_floor_price=-1000.0, energy_market_ceiling_price=14500.0,
                                 fcas_market_ceiling_price=1000.0)
        else:
            self.market.dispatch(price_market_constraints=calc_prices, allow_over_constrained_dispatch_re_run=False)

    def is_regional_demand_meet(self, tolerance=0.5):
        DISPATCHREGIONSUM = self.inputs_manager.DISPATCHREGIONSUM.get_data(self.interval)
        regional_demand = hi.format_regional_demand(DISPATCHREGIONSUM)
        region_summary = self.market.get_region_dispatch_summary()
        region_summary = pd.merge(region_summary, regional_demand, on='region')
        region_summary['calc_demand'] = region_summary['dispatch'] + region_summary['inflow'] \
                                        - region_summary['interconnector_losses'] - \
                                        region_summary['transmission_losses']
        region_summary['diff'] = region_summary['calc_demand'] - region_summary['demand']
        region_summary['no_error'] = region_summary['diff'].abs() < tolerance
        return region_summary['no_error'].all()

    def is_generic_constraint_slack_correct(self):

        def calc_slack(rhs, lhs, type):
            if type == '<=':
                slack = rhs - lhs
            elif type == '>=':
                slack = lhs - rhs
            else:
                slack = 0.0
            if slack < 0.0:
                slack = 0.0
            return slack

        DISPATCHCONSTRAINT = self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)
        generic_cons_slack = self.market._constraints_rhs_and_type['generic']
        generic_cons_slack['slack'] = generic_cons_slack['slack'].abs()
        generic_cons_slack = pd.merge(generic_cons_slack, DISPATCHCONSTRAINT, left_on='set',
                                      right_on='CONSTRAINTID')
        generic_cons_slack['aemo_slack'] = (generic_cons_slack['RHS'] - generic_cons_slack['LHS'])
        generic_cons_slack['aemo_slack'] = \
            generic_cons_slack.apply(lambda x: calc_slack(x['RHS'], x['LHS'], x['type']), axis=1)
        generic_cons_slack['comp'] = (generic_cons_slack['aemo_slack'] - generic_cons_slack['slack']).abs()
        generic_cons_slack['no_error'] = generic_cons_slack['comp'] < 0.9
        return generic_cons_slack['no_error'].all()

    def is_fcas_constraint_slack_correct(self):

        def calc_slack(rhs, lhs, type):
            if type == '<=':
                slack = rhs - lhs
            elif type == '>=':
                slack = lhs - rhs
            else:
                slack = 0.0
            if slack < 0.0:
                slack = 0.0
            return slack

        DISPATCHCONSTRAINT = self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)
        generic_cons_slack = self.market._market_constraints_rhs_and_type['fcas']
        generic_cons_slack['slack'] = generic_cons_slack['slack'].abs()
        generic_cons_slack = pd.merge(generic_cons_slack, DISPATCHCONSTRAINT, left_on='set',
                                      right_on='CONSTRAINTID')
        generic_cons_slack['aemo_slack'] = (generic_cons_slack['RHS'] - generic_cons_slack['LHS'])
        generic_cons_slack['aemo_slack'] = \
            generic_cons_slack.apply(lambda x: calc_slack(x['RHS'], x['LHS'], x['type']), axis=1)
        generic_cons_slack['comp'] = (generic_cons_slack['aemo_slack'] - generic_cons_slack['slack']).abs()
        generic_cons_slack['no_error'] = generic_cons_slack['comp'] < 0.9
        return generic_cons_slack['no_error'].all()

    def all_constraints_presenet(self):
        DISPATCHCONSTRAINT = list(self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)['CONSTRAINTID'])
        fcas = list(self.market._market_constraints_rhs_and_type['fcas']['set'])
        generic = list(self.market._constraints_rhs_and_type['generic']['set'])
        generic = generic + fcas
        return set(DISPATCHCONSTRAINT) < set(generic + [1])

    def get_price_comparison(self):
        energy_prices = self.market.get_energy_prices()
        energy_prices['time'] = self.interval
        energy_prices['service'] = 'energy'
        fcas_prices = self.market.get_fcas_prices()
        fcas_prices['time'] = self.interval
        prices = pd.concat([energy_prices, fcas_prices])

        price_to_service = {'ROP': 'energy', 'RAISE6SECROP': 'raise_6s', 'RAISE60SECROP': 'raise_60s',
                            'RAISE5MINROP': 'raise_5min', 'RAISEREGROP': 'raise_reg', 'LOWER6SECROP': 'lower_6s',
                            'LOWER60SECROP': 'lower_60s', 'LOWER5MINROP': 'lower_5min', 'LOWERREGROP': 'lower_reg'}
        price_columns = list(price_to_service.keys())
        historical_prices = self.inputs_manager.DISPATCHPRICE.get_data(self.interval)
        historical_prices = hf.stack_columns(historical_prices, cols_to_keep=['SETTLEMENTDATE', 'REGIONID'],
                                             cols_to_stack=price_columns, type_name='service',
                                             value_name='RRP')
        historical_prices['service'] = historical_prices['service'].apply(lambda x: price_to_service[x])
        historical_prices = historical_prices.loc[:, ['SETTLEMENTDATE', 'REGIONID', 'service', 'RRP']]
        historical_prices.columns = ['time', 'region', 'service', 'hist_price']
        prices = pd.merge(prices, historical_prices, on=['time', 'region', 'service'])
        prices['error'] = prices['price'] - prices['hist_price']
        return prices

    def get_dispatch_comparison(self):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)
        bounds = DISPATCHLOAD.loc[:, ['DUID'] + self.services]
        bounds.columns = ['unit'] + self.services
        bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=self.services, type_name='service',
                                  value_name='dispatched')
        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        nempy_dispatch = self.market.get_unit_dispatch()
        comp = pd.merge(bounds, nempy_dispatch, 'inner', on=['unit', 'service'])
        comp['diff'] = comp['dispatch'] - comp['dispatched']
        comp = pd.merge(comp, self.market._unit_info.loc[:, ['unit', 'dispatch_type']], on='unit')
        comp['diff'] = np.where((comp['dispatch_type'] == 'load') & (comp['service'] == 'energy'), comp['diff'] * -1, comp['diff'])
        return comp

    def do_fcas_availabilities_match_historical(self):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)
        availabilities = ['RAISE6SECACTUALAVAILABILITY', 'RAISE60SECACTUALAVAILABILITY',
                          'RAISE5MINACTUALAVAILABILITY', 'RAISEREGACTUALAVAILABILITY',
                          'LOWER6SECACTUALAVAILABILITY', 'LOWER60SECACTUALAVAILABILITY',
                          'LOWER5MINACTUALAVAILABILITY', 'LOWERREGACTUALAVAILABILITY']

        availabilities_mapping = {'RAISEREGACTUALAVAILABILITY': 'raise_reg',
                                  'LOWERREGACTUALAVAILABILITY': 'lower_reg',
                                  'RAISE6SECACTUALAVAILABILITY': 'raise_6s',
                                  'RAISE60SECACTUALAVAILABILITY': 'raise_60s',
                                  'RAISE5MINACTUALAVAILABILITY': 'raise_5min',
                                  'LOWER6SECACTUALAVAILABILITY': 'lower_6s',
                                  'LOWER60SECACTUALAVAILABILITY': 'lower_60s',
                                  'LOWER5MINACTUALAVAILABILITY': 'lower_5min'}

        bounds = DISPATCHLOAD.loc[:, ['DUID'] + availabilities]
        bounds.columns = ['unit'] + availabilities

        availabilities = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=availabilities,
                                          type_name='service', value_name='availability')

        bounds = DISPATCHLOAD.loc[:, ['DUID'] + self.services]
        bounds.columns = ['unit'] + self.services

        bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=self.services, type_name='service',
                                  value_name='dispatched')

        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        availabilities['service'] = availabilities['service'].apply(lambda x: availabilities_mapping[x])

        availabilities = pd.merge(availabilities, bounds, on=['unit', 'service'])

        availabilities = availabilities[~(availabilities['dispatched'] - 0.001 > availabilities['availability'])]

        output = self.market.get_fcas_availability()
        output.columns = ['unit', 'service', 'availability_measured']

        availabilities = pd.merge(availabilities, output, 'left', on=['unit', 'service'])

        availabilities['availability_measured'] = availabilities['availability_measured'].fillna(0)

        availabilities['error'] = availabilities['availability_measured'] - availabilities['availability']

        availabilities['match'] = availabilities['error'].abs() < 0.1
        availabilities = availabilities.sort_values('match')
        return availabilities
