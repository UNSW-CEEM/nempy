import pandas as pd
import pytest
import numpy as np
from datetime import datetime, timedelta

from nempy import markets
from nempy.help_functions import helper_functions as hf
from nempy.historical_inputs import mms_db as hi, demand


class SpotMarketBuilder:
    def __init__(self, unit_inputs, interconnector_inputs, constraint_inputs, demand_inputs):

        self.unit_inputs = unit_inputs
        self.interconnector_inputs = interconnector_inputs
        self.constraint_inputs = constraint_inputs
        self.regional_demand_inputs = demand_inputs
        self.fast_start_profiles = None

        unit_info = self.unit_inputs.get_unit_info()
        self.market = markets.SpotMarket(market_regions=['QLD1', 'NSW1', 'VIC1', 'SA1', 'TAS1'], unit_info=unit_info)
        self.market.solver_name = 'CBC'

    def set_solver(self, solver_name):
        self.market.solver_name = solver_name

    def add_unit_bids_to_market(self):
        volume_bids, price_bids = self.unit_inputs.get_processed_bids()
        self.market.set_unit_volume_bids(volume_bids)
        self.market.set_unit_price_bids(price_bids)

    def set_unit_limit_constraints(self):
        unit_bid_limit = self.unit_inputs.get_unit_bid_availability()
        cost = self.constraint_inputs.get_constraint_violation_prices()['unit_capacity']
        self.market.set_unit_bid_capacity_constraints(unit_bid_limit, violation_cost=cost)
        unit_uigf_limit = self.unit_inputs.get_unit_uigf_limits()
        cost = self.constraint_inputs.get_constraint_violation_prices()['uigf']
        self.market.set_unconstrained_intermittent_generation_forecast_constraint(unit_uigf_limit, violation_cost=cost)

    def set_ramp_rate_limits(self):
        ramp_rates = self.unit_inputs.get_bid_ramp_rates()
        scada_ramp_rates = self.unit_inputs.get_scada_ramp_rates()
        fast_start_profiles = self.unit_inputs.get_fast_start_profiles_for_dispatch()
        fast_start_profiles = fast_start_profiles.loc[:, ['unit', 'current_mode']]
        cost = self.constraint_inputs.get_constraint_violation_prices()['ramp_rate']
        self.market.set_unit_ramp_rate_constraints(
            ramp_rates, scada_ramp_rates, fast_start_profiles,
            run_type="fast_start_first_run", violation_cost=cost
        )

    def set_fast_start_constraints(self):
        self.market.dispatch()
        dispatch = self.market.get_unit_dispatch()
        cost = self.constraint_inputs.get_constraint_violation_prices()['fast_start']
        self.fast_start_profiles = self.unit_inputs.get_fast_start_profiles_for_dispatch(dispatch)
        cols = ['unit', 'end_mode', 'time_in_end_mode', 'mode_two_length',
                'mode_four_length', 'min_loading']
        fsp = self.fast_start_profiles.loc[:, cols]
        self.market.set_fast_start_constraints(fsp, violation_cost=cost)

        ramp_rates = self.unit_inputs.get_bid_ramp_rates()
        scada_ramp_rates = self.unit_inputs.get_scada_ramp_rates()
        cols = ['unit', 'end_mode', 'time_since_end_of_mode_two', 'min_loading']
        fast_start_profiles = self.fast_start_profiles.loc[:, cols]
        cost = self.constraint_inputs.get_constraint_violation_prices()['ramp_rate']
        self.market.set_unit_ramp_rate_constraints(
            ramp_rates, scada_ramp_rates, fast_start_profiles,
            run_type="fast_start_second_run", violation_cost=cost
        )
        cost = self.constraint_inputs.get_constraint_violation_prices()['fcas_profile']
        scada_ramp_rates = self.unit_inputs.get_scada_ramp_rates(inlude_initial_output=True)
        self.market.set_joint_ramping_constraints_reg(
            scada_ramp_rates, fast_start_profiles, run_type="fast_start_second_run", violation_cost=cost
        )

    def set_unit_fcas_constraints(self):
        self.unit_inputs.add_fcas_trapezium_constraints()
        cost = self.constraint_inputs.get_constraint_violation_prices()['fcas_max_avail']
        fcas_availability = self.unit_inputs.get_fcas_max_availability()
        self.market.set_fcas_max_availability(fcas_availability, violation_cost=cost)
        cost = self.constraint_inputs.get_constraint_violation_prices()['fcas_profile']
        regulation_trapeziums = self.unit_inputs.get_fcas_regulation_trapeziums()
        self.market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums, violation_cost=cost)
        fast_start_profiles = self.unit_inputs.get_fast_start_profiles_for_dispatch()
        fast_start_profiles = fast_start_profiles.loc[:, ['unit', 'current_mode']]
        scada_ramp_rates = self.unit_inputs.get_scada_ramp_rates(inlude_initial_output=True)
        self.market.set_joint_ramping_constraints_reg(
            scada_ramp_rates, fast_start_profiles, run_type="fast_start_first_run", violation_cost=cost
        )
        contingency_trapeziums = self.unit_inputs.get_contingency_services()
        self.market.set_joint_capacity_constraints(contingency_trapeziums, violation_cost=cost)

    def set_region_demand_constraints(self):
        regional_demand = self.regional_demand_inputs.get_operational_demand()
        cost = self.constraint_inputs.get_constraint_violation_prices()['regional_demand']
        self.market.set_demand_constraints(regional_demand, violation_cost=cost)

    def add_interconnectors_to_market(self):
        interconnectors = self.interconnector_inputs.get_interconnector_definitions()
        loss_functions, interpolation_break_points = self.interconnector_inputs.get_interconnector_loss_model()
        self.market.set_interconnectors(interconnectors)
        self.market.set_interconnector_losses(loss_functions, interpolation_break_points)

    def add_generic_constraints_with_fcas_requirements_interface(self):
        fcas_requirements = self.constraint_inputs.get_fcas_requirements()
        cost = self.constraint_inputs.get_violation_costs()
        self.market.set_fcas_requirements_constraints(fcas_requirements, violation_cost=cost)
        generic_rhs = self.constraint_inputs.get_rhs_and_type_excluding_regional_fcas_constraints()
        self.market.set_generic_constraints(generic_rhs, violation_cost=cost)
        unit_generic_lhs = self.constraint_inputs.get_unit_lhs()
        self.market.link_units_to_generic_constraints(unit_generic_lhs)
        interconnector_generic_lhs = self.constraint_inputs.get_interconnector_lhs()
        self.market.link_interconnectors_to_generic_constraints(interconnector_generic_lhs)

    def add_generic_constraints(self):
        cost = self.constraint_inputs.get_violation_costs()
        generic_rhs = self.constraint_inputs.get_rhs_and_type()
        self.market.set_generic_constraints(generic_rhs, violation_cost=cost)
        unit_generic_lhs = self.constraint_inputs.get_unit_lhs()
        self.market.link_units_to_generic_constraints(unit_generic_lhs)
        interconnector_generic_lhs = self.constraint_inputs.get_interconnector_lhs()
        self.market.link_interconnectors_to_generic_constraints(interconnector_generic_lhs)
        regions_generic_lhs = self.constraint_inputs.get_region_lhs()
        self.market.link_regions_to_generic_constraints(regions_generic_lhs)

    def dispatch(self):
        if self.constraint_inputs.is_over_constrained_dispatch_rerun():
            self.market.dispatch(allow_over_constrained_dispatch_re_run=True,
                                 energy_market_floor_price=-1000.0, energy_market_ceiling_price=14500.0,
                                 fcas_market_ceiling_price=1000.0)
        else:
            self.market.dispatch(allow_over_constrained_dispatch_re_run=False)

    def get_market_object(self):
        return self.market


class MarketOverrider:
    def __init__(self, market, mms_db, interval, raw_inputs_loader):
        self.services = ['TOTALCLEARED', 'LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC',
                         'LOWERREG', 'RAISEREG']

        self.service_name_mapping = {'TOTALCLEARED': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg',
                                     'RAISE6SEC': 'raise_6s', 'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min',
                                     'LOWER6SEC': 'lower_6s', 'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min',
                                     'ENERGY': 'energy'}

        if interval > "2022/01/01 00:00:00":
            self.service_name_mapping.update(
                {'RAISE1SEC': 'raise_1s', 'LOWER1SEC': 'lower_1s'}
            )
            self.services += ['RAISE1SEC', 'LOWER1SEC']

        self.inputs_manager = mms_db
        self.interval = interval
        self.raw_inputs_loader = raw_inputs_loader
        self.market = market

    def set_unit_dispatch_to_historical_values(self, wiggle_room=0.001):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)
        trader_type = self.raw_inputs_loader.get_unit_initial_conditions().loc[:, ['DUID', 'TRADERTYPE']]
        DISPATCHLOAD = pd.merge(DISPATCHLOAD, trader_type, on='DUID')

        bounds = DISPATCHLOAD.loc[:, ['DUID', 'TRADERTYPE'] + self.services]
        bounds.columns = ['unit', 'trader_type'] + self.services

        bounds = hf.stack_columns(bounds, cols_to_keep=['unit', 'trader_type'], cols_to_stack=self.services,
                                  type_name='service', value_name='dispatched')

        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        decision_variables = self.market._decision_variables['bids'].copy()

        decision_variables = pd.merge(decision_variables, bounds, on=['unit', 'service'])

        decision_variables_first_bid = decision_variables.groupby(['unit', 'service', 'dispatch_type'], as_index=False).first()

        bdu = \
            decision_variables_first_bid[
                (decision_variables_first_bid['trader_type'] == 'BIDIRECTIONAL') &
                (decision_variables_first_bid['service'] == 'energy')
            ].copy()

        bdu_fcas = \
            decision_variables_first_bid[
                (decision_variables_first_bid['trader_type'] == 'BIDIRECTIONAL') &
                (decision_variables_first_bid['service'] != 'energy')
            ].copy()

        not_bdu = \
            decision_variables_first_bid[
                ~(decision_variables_first_bid['trader_type'] == 'BIDIRECTIONAL')
                ].copy()

        bdu_gen = bdu[bdu['dispatch_type'] == 'generator'].copy()
        bdu_load = bdu[bdu['dispatch_type'] == 'load'].copy()

        bdu_gen['dispatched'] = np.where(bdu_gen['dispatched'] < 0.0, 0.0, bdu_gen['dispatched'])

        bdu_load['dispatched'] = np.where(bdu_load['dispatched'] > 0.0, 0.0, bdu_load['dispatched'].abs())

        bdu_fcas_total = \
            decision_variables[
                (decision_variables['trader_type'] == 'BIDIRECTIONAL') &
                (decision_variables['service'] != 'energy')
            ].copy()

        bdu_fcas_total = bdu_fcas_total.groupby(['unit', 'service', 'dispatch_type'], as_index=False)['value'].sum()

        bdu_fcas_dispatched = bdu_fcas_total[bdu_fcas_total['value'] > 0.0].copy()
        if not bdu_fcas_dispatched[bdu_fcas_dispatched.duplicated(['unit', 'service'])].empty:
            return False

        bdu_fcas_total = bdu_fcas_total.rename(columns={'value': 'total_fcas_dispatch'})

        bdu_fcas = pd.merge(
            bdu_fcas,
            bdu_fcas_total,
            'left',
            on=['unit', 'service', 'dispatch_type']
        )

        bdu_fcas_comp = bdu_fcas.groupby(['unit', 'service'], as_index=False).agg({
            'dispatched': 'first', 'total_fcas_dispatch': 'first'
        })
        fcas_not_dispatched = bdu_fcas_comp[
            (bdu_fcas_comp['dispatched'] > 0.0) &
            (bdu_fcas_comp['total_fcas_dispatch'] == 0.0)
        ].copy()
        if not fcas_not_dispatched.empty:
            return False

        bdu_fcas['dispatched'] = np.where(bdu_fcas['total_fcas_dispatch'] <= 0.0, 0.0, bdu_fcas['dispatched'])
        bdu_fcas = bdu_fcas.drop(columns=['total_fcas_dispatch'])

        decision_variables_first_bid = pd.concat([
            not_bdu,
            bdu_gen,
            bdu_load,
            bdu_fcas
        ])

        def last_bids(df):
            return df.iloc[1:]

        decision_variables_remaining_bids = \
            decision_variables.groupby(['unit', 'service', 'dispatch_type'], as_index=False).apply(last_bids)

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

        return True

    def set_interconnector_flow_to_historical_values(self, wiggle_room=0.1):
        # Historical interconnector dispatch
        DISPATCHINTERCONNECTORRES = self.inputs_manager.DISPATCHINTERCONNECTORRES.get_data(self.interval)
        interconnector_flow = DISPATCHINTERCONNECTORRES.loc[:, ['INTERCONNECTORID', 'MWFLOW']]
        interconnector_flow.columns = ['interconnector', 'flow']
        interconnector_flow['link'] = interconnector_flow['interconnector']
        interconnector_flow['link'] = np.where(interconnector_flow['interconnector'] == 'T-V-MNSP1',
                                               np.where(interconnector_flow['flow'] >= 0.0, 'BLNKTAS', 'BLNKVIC'),
                                               interconnector_flow['link'])

        flow_variables = self.market._decision_variables['interconnectors']
        flow_variables = pd.merge(flow_variables, interconnector_flow, 'left', on=['interconnector', 'link'])
        flow_variables = flow_variables.fillna(0.0)
        flow_variables['flow'] = np.where(flow_variables['link'] != flow_variables['interconnector'],
                                          flow_variables['flow'].abs(), flow_variables['flow'])
        flow_variables['lower_bound'] = flow_variables['flow'] - wiggle_room
        flow_variables['upper_bound'] = flow_variables['flow'] + wiggle_room
        flow_variables = flow_variables.drop(['flow'], axis=1)
        self.market._decision_variables['interconnectors'] = flow_variables


class MarketChecker:
    def __init__(self, market, mms_db, xml_cache, interval, unit_inputs=None):
        self.services = ['TOTALCLEARED', 'LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC',
                         'LOWERREG', 'RAISEREG']

        self.service_name_mapping = {'TOTALCLEARED': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg',
                                     'RAISE6SEC': 'raise_6s', 'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min',
                                     'LOWER6SEC': 'lower_6s', 'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min',
                                     'ENERGY': 'energy'}

        self.inputs_manager = mms_db
        self.xml = xml_cache
        self.unit_inputs = unit_inputs
        self.interval = interval

        self.market = market

    def all_dispatch_units_and_services_have_decision_variables(self, wiggle_room=0.001):
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

    def is_regional_demand_meet(self, tolerance=0.5):
        DISPATCHREGIONSUM = self.inputs_manager.DISPATCHREGIONSUM.get_data(self.interval)
        regional_demand = demand._format_regional_demand(DISPATCHREGIONSUM)
        region_summary = self.market.get_region_dispatch_summary()
        region_summary = pd.merge(region_summary, regional_demand, on='region')
        region_summary['calc_demand'] = region_summary['dispatch'] + region_summary['inflow'] \
            - region_summary['interconnector_losses'] - region_summary['transmission_losses']
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
        generic_cons_slack = generic_cons_slack.sort_values('comp', ascending=False)
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
        generic_cons_slack = generic_cons_slack.sort_values('comp', ascending=False)
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
        comp['diff'] = np.where((comp['dispatch_type'] == 'load') & (comp['service'] == 'energy'), comp['diff'] * -1,
                                comp['diff'])
        return comp

    def do_fcas_availabilities_match_historical(self):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)
        availabilities = ['RAISE6SECACTUALAVAILABILITY', 'RAISE60SECACTUALAVAILABILITY',
                          'RAISE5MINACTUALAVAILABILITY', 'RAISEREGACTUALAVAILABILITY',
                          'LOWER6SECACTUALAVAILABILITY', 'LOWER60SECACTUALAVAILABILITY',
                          'LOWER5MINACTUALAVAILABILITY', 'LOWERREGACTUALAVAILABILITY',
                          'RAISE1SECACTUALAVAILABILITY', 'LOWER1SECACTUALAVAILABILITY']

        availabilities_mapping = {'RAISEREGACTUALAVAILABILITY': 'raise_reg',
                                  'LOWERREGACTUALAVAILABILITY': 'lower_reg',
                                  'RAISE6SECACTUALAVAILABILITY': 'raise_6s',
                                  'RAISE1SECACTUALAVAILABILITY': 'raise_1s',
                                  'RAISE60SECACTUALAVAILABILITY': 'raise_60s',
                                  'RAISE5MINACTUALAVAILABILITY': 'raise_5min',
                                  'LOWER6SECACTUALAVAILABILITY': 'lower_6s',
                                  'LOWER1SECACTUALAVAILABILITY': 'lower_1s',
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

        #availabilities = pd.merge(availabilities, bounds, on=['unit', 'service'])

        #availabilities = availabilities[~(availabilities['dispatched'] - 0.001 > availabilities['availability'])]

        output = self.market.get_fcas_availability()
        output.columns = ['unit', 'service', 'availability_measured']

        availabilities = pd.merge(availabilities, output, 'left', on=['unit', 'service'])

        availabilities['availability_measured'] = availabilities['availability_measured'].fillna(0)

        availabilities['error'] = availabilities['availability_measured'] - availabilities['availability']

        availabilities['match'] = availabilities['error'].abs() < 0.1
        availabilities = availabilities.sort_values('match')

        return availabilities

    def measured_violation_equals_historical_violation(self, historical_name, nempy_constraints):
        measured = 0.0
        for name in nempy_constraints:
            # v = self.violation_details(name)
            if name in self.market.get_constraint_set_names():
                measured += self.market.get_elastic_constraints_violation_degree(name)
        historical = self.xml.get_violations()[historical_name]
        return measured == pytest.approx(historical, abs=0.1)

    def violation_details(self, constraint_set):
        violations = self.market._decision_variables[constraint_set + '_deficit']
        violations = violations[violations['value'].abs() > 0.0]
        lhs = self.market._lhs_coefficients[constraint_set + '_deficit']
        violations = pd.merge(violations, lhs, on='variable_id')
        rhs_and_type = self.market._constraints_rhs_and_type[constraint_set]
        return pd.merge(violations, rhs_and_type, on='constraint_id')


def get_next_interval(interval):
    interval = datetime.strptime(interval, '%Y/%m/%d %H:%M:%S')
    next_interval = interval + timedelta(minutes=5)
    next_interval = next_interval.strftime('%Y/%m/%d %H:%M:%S')
    return next_interval
