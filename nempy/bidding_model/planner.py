import pandas as pd
import numpy as np
import math
from itertools import product
from mip import Model, xsum, maximize, INF, BINARY
from causalnex.structure.pytorch import DAGRegressor


class DispatchPlanner:
    def __init__(self, dispatch_interval, historical_data, forward_data, train_pct=0.1, demand_delta_steps=10):
        self.dispatch_interval = dispatch_interval
        self.historical_data = historical_data
        self.forward_data = forward_data
        self.planning_horizon = len(self.forward_data.index)
        self.regional_markets = []
        self.price_traces_by_market = {}
        self.units = []
        self.unit_energy_market_mapping = {}
        self.unit_fcas_market_mapping = {}
        self.model = Model(solver_name='CBC', sense='MAX')
        self.unit_in_flow_variables = {}
        self.unit_out_flow_variables = {}
        self.units_with_storage = []
        self.unit_storage_input_capacity = {}
        self.unit_storage_output_capacity = {}
        self.unit_storage_input_efficiency = {}
        self.unit_storage_output_efficiency = {}
        self.unit_storage_mwh = {}
        self.unit_storage_initial_mwh = {}
        self.unit_storage_level_variables = {}
        self.unit_output_fcas_variables = {}
        self.unit_input_fcas_variables = {}
        self.unit_initial_mw = {}
        self.market_dispatch_variables = {}
        self.market_net_dispatch_variables = {}
        self.nominal_price_forecast = {}
        self.train_pct = train_pct
        self.demand_delta_steps = demand_delta_steps
        self.expected_regions = ['qld', 'nsw', 'vic', 'sa', 'tas', 'mainland']
        self.expected_service = ['energy',
                                 'raise_5_min', 'raise_60_second', 'raise_6_second', 'raise_regulation',
                                 'lower_5_min', 'lower_60_second', 'lower_6_second', 'lower_regulation']
        self.unit_commitment_vars = {}
        self.unit_capacity = {}
        self.unit_min_loading = {}
        self.unit_min_down_time = {}

    def add_regional_market(self, region, service):
        market_name = region + '-' + service
        self.regional_markets.append(market_name)

        forward_dispatch = self._get_forward_dispatch_trace(region, service, self.forward_data)
        forward_data = pd.merge(self.forward_data, forward_dispatch, on='interval')

        positive_rate, positive_dispatch, negative_rate, negative_dispatch = \
            self._marginal_market_trade(region, service, forward_data)

        if region not in self.market_dispatch_variables:
            self.market_dispatch_variables[region] = {}
        self.market_dispatch_variables[region][service] = {}
        if region not in self.market_net_dispatch_variables:
            self.market_net_dispatch_variables[region] = {}
        self.market_net_dispatch_variables[region][service] = {}

        for i in range(0, self.planning_horizon):
            self.market_dispatch_variables[region][service][i] = {}
            self.market_dispatch_variables[region][service][i]['positive'] = {}
            self.market_dispatch_variables[region][service][i]['negative'] = {}
            self.market_net_dispatch_variables[region][service][i] = {}

            if len(positive_rate) > 0:
                for dispatch, rate in positive_rate[i].items():
                    dispatch_var_name = "dispatch_{}_{}_positive_{}".format(market_name, i, dispatch)
                    self.market_dispatch_variables[region][service][i]['positive'][dispatch] = \
                        self.model.add_var(name=dispatch_var_name, lb=0.0, ub=positive_dispatch[i][dispatch], obj=rate)

            if len(negative_rate) > 0:
                for dispatch, rate in negative_rate[i].items():
                    dispatch_var_name = "dispatch_{}_{}_negative_{}".format(market_name, i, dispatch)
                    self.market_dispatch_variables[region][service][i]['negative'][dispatch] = \
                        self.model.add_var(name=dispatch_var_name, lb=0.0, ub=negative_dispatch[i][dispatch], obj=rate)

            dispatch_var_name = "net_dispatch_{}_{}".format(market_name, i)
            self.market_net_dispatch_variables[region][service][i] = \
                self.model.add_var(name=dispatch_var_name, lb=-1.0 * INF, ub=INF)

            positive_vars = list(self.market_dispatch_variables[region][service][i]['positive'].values())
            negative_vars = list(self.market_dispatch_variables[region][service][i]['negative'].values())
            self.model += xsum([-1 * self.market_net_dispatch_variables[region][service][i]] + positive_vars +
                               [-1 * var for var in negative_vars]) == 0.0

    def _update_price_forecast(self, market, forward_data):
        region = market.split('-')[0]
        service = market.split('-')[1]

        forward_dispatch = self._get_forward_dispatch_trace(region, service, self.forward_data)
        forward_data = pd.merge(self.forward_data, forward_dispatch, on='interval')

        positive_rate, positive_dispatch, negative_rate, negative_dispatch = self._marginal_market_trade(region,
                                                                                                         service,
                                                                                                         forward_data)
        for i in range(0, self.planning_horizon):

            for dispatch, rate in positive_rate[i].items():
                dispatch_var_name = "dispatch_{}_{}_positive_{}".format(market, i, dispatch)
                var = self.model.var_by_name(name=dispatch_var_name)
                var.obj = rate

            if len(negative_rate) > 0:
                for dispatch, rate in negative_rate[i].items():
                    dispatch_var_name = "dispatch_{}_{}_negative_{}".format(market, i, dispatch)
                    var = self.model.var_by_name(name=dispatch_var_name)
                    var.obj = rate

    def _get_revenue_traces(self, region, service, forward_data):
        target_column_name = region + '-' + service

        forecaster = Forecaster()

        cols_to_drop = []
        for region_option, service_option in product(self.expected_regions, self.expected_service):
            col = region_option + '-' + service_option
            if col != target_column_name and col in self.historical_data.columns:
                cols_to_drop.append(col)

        historical_data = self.historical_data.drop(columns=cols_to_drop)
        cols_to_drop = [col for col in cols_to_drop if col in forward_data.columns]
        forward_data = forward_data.drop(columns=cols_to_drop)

        forecaster.train(data=historical_data, train_sample_fraction=self.train_pct, target_col=target_column_name)

        if service == 'energy':
            price_traces = forecaster.price_forecast(forward_data=forward_data, region=region,
                                                     market=target_column_name,
                                                     min_delta=self._get_market_out_flow_capacity(region, service),
                                                     max_delta=self._get_market_in_flow_capacity(region, service),
                                                     steps=self.demand_delta_steps)
        else:
            price_traces = forecaster.price_forecast(forward_data=forward_data, region=region,
                                                     market=target_column_name,
                                                     min_delta=0,
                                                     max_delta=self._get_market_fcas_capacity(region, service),
                                                     steps=self.demand_delta_steps)

        self.nominal_price_forecast[target_column_name] = price_traces

        for col in price_traces.columns:
            if col != 'interval':
                price_traces[col] = price_traces[col] * (col + 0.00001)
        return price_traces

    def _get_market_in_flow_capacity(self, region, service):
        capacity = 0.0
        for unit_name, unit_capacity in self.unit_capacity.items():
            if self.unit_energy_market_mapping[unit_name] == region + '-' + service:
                capacity += unit_capacity
        return capacity

    def _get_market_out_flow_capacity(self, region, service):
        capacity = 0.0
        for unit_name, market_out_flow_variable in self.unit_in_flow_variables.items():
            if (self.unit_energy_market_mapping[unit_name] == region + '-' + service and
                    'market_to_unit' in market_out_flow_variable[0]):
                capacity -= market_out_flow_variable[0]['market_to_unit'].ub
        return capacity

    def _get_market_fcas_capacity(self, region, service):
        capacity = 0.0

        for unit_name, vars_by_service in self.unit_output_fcas_variables.items():
            if ((unit_name in self.unit_fcas_market_mapping[service] and
                 self.unit_fcas_market_mapping[service][unit_name] == region)
                    and service in vars_by_service):
                capacity += vars_by_service[service][0].ub

        for unit_name, vars_by_service in self.unit_input_fcas_variables.items():
            if ((unit_name in self.unit_fcas_market_mapping[service] and
                 self.unit_fcas_market_mapping[service][unit_name] == region)
                    and service in vars_by_service):
                capacity += vars_by_service[service][0].ub

        return capacity

    def _get_forward_dispatch_trace(self, region, service, forward_data):
        target_column_name = region + '-' + service + '-fleet-dispatch'

        forecaster = Forecaster()

        cols_to_drop = []
        for region_option, service_option in product(self.expected_regions, self.expected_service):
            col = region_option + '-' + service_option
            if col in self.historical_data.columns:
                cols_to_drop.append(col)
            col = region_option + '-' + service_option + '-fleet-dispatch'
            if col in self.historical_data.columns and col != target_column_name:
                cols_to_drop.append(col)

        historical_data = self.historical_data.drop(columns=cols_to_drop)
        cols_to_drop = [col for col in cols_to_drop if col in forward_data.columns]
        forward_data = forward_data.drop(columns=cols_to_drop)

        forecaster.train(data=historical_data, train_sample_fraction=0.1, target_col=target_column_name)
        forward_dispatch = forecaster.base_forecast(forward_data=forward_data)

        return forward_dispatch

    def get_nominal_price_forecast(self, region, service):
        return self.nominal_price_forecast[region + '-' + service]

    def _marginal_market_trade(self, region, service, forward_data):
        revenue_trace = self._get_revenue_traces(region, service, forward_data)
        value_columns = [col for col in revenue_trace.columns if col != 'interval']
        stacked = pd.melt(revenue_trace, id_vars=['interval'], value_vars=value_columns,
                          var_name='dispatch', value_name='revenue')

        positive = stacked[stacked['dispatch'] >= 0.0]
        negative = stacked[stacked['dispatch'] <= 0.0].copy()
        negative['dispatch'] = negative['dispatch'] * -1.0

        positive = positive.sort_values('dispatch')
        negative = negative.sort_values('dispatch')

        positive['marginal_revenue'] = positive.groupby('interval', as_index=False)['revenue'].diff()
        negative['marginal_revenue'] = negative.groupby('interval', as_index=False)['revenue'].diff()

        positive['marginal_dispatch'] = positive.groupby('interval', as_index=False)['dispatch'].diff()
        negative['marginal_dispatch'] = negative.groupby('interval', as_index=False)['dispatch'].diff()

        positive = positive[positive['dispatch'] != 0.0]
        negative = negative[negative['dispatch'] != 0.0]

        positive['marginal_rate'] = positive['marginal_revenue'] / positive['marginal_dispatch']
        negative['marginal_rate'] = negative['marginal_revenue'] / negative['marginal_dispatch']

        positive = positive.set_index(['interval', 'dispatch']).loc[:, ['marginal_rate', 'marginal_dispatch']]
        negative = negative.set_index(['interval', 'dispatch']).loc[:, ['marginal_rate', 'marginal_dispatch']]

        positive_rate = positive.groupby(level=0).apply(lambda df: df.xs(df.name).marginal_rate.to_dict()).to_dict()
        positive_dispatch = positive.groupby(level=0).apply(
            lambda df: df.xs(df.name).marginal_dispatch.to_dict()).to_dict()

        negative_rate = negative.groupby(level=0).apply(lambda df: df.xs(df.name).marginal_rate.to_dict()).to_dict()
        negative_dispatch = negative.groupby(level=0).apply(
            lambda df: df.xs(df.name).marginal_dispatch.to_dict()).to_dict()

        return positive_rate, positive_dispatch, negative_rate, negative_dispatch

    def add_unit(self, name, region, initial_mw=0.0):
        self.units.append(name)
        self.unit_energy_market_mapping[name] = region + '-energy'
        self.unit_in_flow_variables[name] = {}
        self.unit_out_flow_variables[name] = {}
        self.unit_output_fcas_variables[name] = {}
        self.unit_input_fcas_variables[name] = {}
        self.unit_initial_mw[name] = initial_mw

        for i in range(0, self.planning_horizon):
            self.unit_in_flow_variables[name][i] = {}
            self.unit_out_flow_variables[name][i] = {}

    def set_unit_fcas_region(self, unit_name, service, region):
        if service not in self.unit_fcas_market_mapping:
            self.unit_fcas_market_mapping[service] = {}
        self.unit_fcas_market_mapping[service][unit_name] = region

    def add_unit_to_market_flow(self, unit_name, capacity):
        self.unit_capacity[unit_name] = capacity
        for i in range(0, self.planning_horizon):
            var_name = "{}_unit_to_market_{}".format(unit_name, i)
            self.unit_out_flow_variables[unit_name][i]['unit_to_market'] = self.model.add_var(name=var_name,
                                                                                              ub=capacity)

    def add_unit_minimum_operating_level(self, unit_name, min_loading, shutdown_ramp_rate, start_up_ramp_rate,
                                         min_up_time, min_down_time, initial_state, initial_up_time,
                                         initial_down_time):
        """Unit commitment constraints are the Tight formulation from Knueven et al. On Mixed Integer Programming
        Formulations for Unit Commitment."""

        startup_max_output = self._mw_per_hour_to_mw_per_interval(start_up_ramp_rate)
        shutdown_max_output = self._mw_per_hour_to_mw_per_interval(shutdown_ramp_rate)
        if startup_max_output < min_loading:
            raise ValueError()
        if shutdown_max_output < min_loading:
            raise ValueError()

        self.unit_commitment_vars[unit_name] = {}
        self.unit_commitment_vars[unit_name]['state'] = {}
        self.unit_commitment_vars[unit_name]['startup_status'] = {}
        self.unit_commitment_vars[unit_name]['shutdown_status'] = {}
        self.unit_min_loading[unit_name] = min_loading
        self.unit_min_down_time[unit_name] = min_down_time

        self._create_state_variables(unit_name)
        self._add_state_variable_constraint(unit_name, initial_state)
        if initial_state == 1:
            self._add_initial_up_time_constraint(unit_name, min_up_time, initial_up_time)
        elif initial_state == 0:
            self._add_initial_down_time_constraint(unit_name, min_down_time, initial_down_time)
        self._add_min_up_time_constraint(unit_name, min_up_time)
        self._add_min_down_time_constraint(unit_name, min_down_time)
        self._update_continuous_production_variable_upper_bound(unit_name, min_loading)
        self._add_start_up_and_shut_down_ramp_rates(unit_name, min_loading, startup_max_output, shutdown_max_output)
        self._add_generation_limit_constraint(unit_name)

    def _mw_per_hour_to_mw_per_interval(self, mw_per_hour):
        return mw_per_hour * (self.dispatch_interval / 60)

    def _create_state_variables(self, unit_name):
        for i in range(0, self.planning_horizon):
            self.unit_commitment_vars[unit_name]['state'][i] = self.model.add_var(var_type=BINARY)
            self.unit_commitment_vars[unit_name]['startup_status'][i] = self.model.add_var(var_type=BINARY)
            self.unit_commitment_vars[unit_name]['shutdown_status'][i] = self.model.add_var(var_type=BINARY)

    def _add_state_variable_constraint(self, unit_name, initial_state):
        for i in range(0, self.planning_horizon):
            if i == 0:
                self.model += (self.unit_commitment_vars[unit_name]['state'][i] - initial_state -
                               self.unit_commitment_vars[unit_name]['startup_status'][i] +
                               self.unit_commitment_vars[unit_name]['shutdown_status'][i] == 0)
            else:
                self.model += (self.unit_commitment_vars[unit_name]['state'][i] -
                               self.unit_commitment_vars[unit_name]['state'][i - 1] -
                               self.unit_commitment_vars[unit_name]['startup_status'][i] +
                               self.unit_commitment_vars[unit_name]['shutdown_status'][i] == 0)

    def _add_initial_up_time_constraint(self, unit_name, min_up_time, initial_up_time):
        min_up_time_in_intervals = self._minutes_to_intervals_round_up(min_up_time)
        initial_up_time_in_intervals = self._minutes_to_intervals_round_up(initial_up_time)
        remaining_up_time = max(0, min_up_time_in_intervals - initial_up_time_in_intervals)
        remaining_up_time = min(remaining_up_time, self.planning_horizon)
        status_vars = []
        for i in range(0, remaining_up_time):
            status_vars.append(self.unit_commitment_vars[unit_name]['state'][i])

        if len(status_vars) > 0:
            self.model += xsum(status_vars) == remaining_up_time

    def _minutes_to_intervals_round_down(self, minutes):
        return math.floor(minutes / self.dispatch_interval)

    def _minutes_to_intervals_round_up(self, minutes):
        return math.ceil(minutes / self.dispatch_interval)

    def _add_initial_down_time_constraint(self, unit_name, min_down_time, initial_down_time):
        min_down_time_in_intervals = self._minutes_to_intervals_round_up(min_down_time)
        initial_down_time_in_intervals = self._minutes_to_intervals_round_up(initial_down_time)
        remaining_down_time = max(0, min_down_time_in_intervals - initial_down_time_in_intervals)
        remaining_down_time = min(remaining_down_time, self.planning_horizon)
        status_vars = []
        for i in range(0, remaining_down_time):
            status_vars.append(self.unit_commitment_vars[unit_name]['state'][i])

        self.model += xsum(status_vars) == 0

    def _add_min_up_time_constraint(self, unit_name, min_up_time):
        min_up_time_in_intervals = self._minutes_to_intervals_round_up(min_up_time)
        for i in range(min_up_time_in_intervals, self.planning_horizon):
            startup_status_vars = []
            for j in range(i - min_up_time_in_intervals + 1, i):
                startup_status_vars.append(self.unit_commitment_vars[unit_name]['startup_status'][j])
            self.model += xsum(startup_status_vars) <= self.unit_commitment_vars[unit_name]['state'][i]

    def _add_min_down_time_constraint(self, unit_name, min_down_time):
        min_down_time_in_intervals = self._minutes_to_intervals_round_up(min_down_time)
        for i in range(min_down_time_in_intervals, self.planning_horizon):
            shutdown_status_vars = []
            for j in range(i - min_down_time_in_intervals + 1, i):
                shutdown_status_vars.append(self.unit_commitment_vars[unit_name]['shutdown_status'][j])
            self.model += xsum(shutdown_status_vars) <= 1 - self.unit_commitment_vars[unit_name]['state'][i]

    def _update_continuous_production_variable_upper_bound(self, unit_name, min_loading):
        for i in range(0, self.planning_horizon):
            self.unit_out_flow_variables[unit_name][i]['unit_to_market'].ub = self.unit_capacity[unit_name] - \
                                                                              min_loading

    def _add_start_up_and_shut_down_ramp_rates(self, unit_name, min_loading, startup_max_output, shutdown_max_output):
        continuous_production_capacity = self.unit_capacity[unit_name] - min_loading
        startup_coefficient = self.unit_capacity[unit_name] - startup_max_output
        shutdown_coefficient = max(startup_max_output - shutdown_max_output, 0)
        shutdown_coefficient_2 = self.unit_capacity[unit_name] - shutdown_max_output
        startup_coefficient_2 = max(shutdown_max_output - startup_max_output, 0)

        for i in range(0, self.planning_horizon - 1):
            self.model += (self.unit_out_flow_variables[unit_name][i]['unit_to_market'] -
                           continuous_production_capacity * self.unit_commitment_vars[unit_name]['state'][i] +
                           startup_coefficient * self.unit_commitment_vars[unit_name]['startup_status'][i] +
                           shutdown_coefficient * self.unit_commitment_vars[unit_name]['shutdown_status'][i + 1] <= 0)

            self.model += (self.unit_out_flow_variables[unit_name][i]['unit_to_market'] -
                           continuous_production_capacity * self.unit_commitment_vars[unit_name]['state'][i] +
                           shutdown_coefficient_2 * self.unit_commitment_vars[unit_name]['shutdown_status'][i + 1] +
                           startup_coefficient_2 * self.unit_commitment_vars[unit_name]['startup_status'][i] <= 0)

    def _add_generation_limit_constraint(self, unit_name):
        for i in range(0, self.planning_horizon):
            self.model += (self.unit_out_flow_variables[unit_name][i]['unit_to_market'] -
                           self.unit_commitment_vars[unit_name]['state'][i] *
                           (self.unit_capacity[unit_name] - self.unit_min_loading[unit_name])) <= 0.0

    def add_ramp_rates(self, unit_name, ramp_up_rate, ramp_down_rate):
        ramp_up_rate = self._mw_per_minute_to_mw_per_interval(ramp_up_rate)
        ramp_down_rate = self._mw_per_minute_to_mw_per_interval(ramp_down_rate)
        self._add_ramping_constraints(unit_name, ramp_up_rate, ramp_down_rate)

    def _add_ramping_constraints(self, unit_name, max_ramp_up, max_ramp_down):
        min_loading = self.unit_min_loading
        for i in range(0, self.planning_horizon):
            if i == 0:
                self.model += (self.unit_commitment_vars[unit_name]['unit_to_market'][i] -
                               max(0, self.unit_initial_mw[unit_name] - min_loading) -
                               max_ramp_up <= 0)
                self.model += (max(0, self.unit_initial_mw[unit_name] - min_loading) -
                               self.unit_commitment_vars[unit_name]['unit_to_market'][i] -
                               max_ramp_down <= 0)
            else:
                self.model += (self.unit_commitment_vars[unit_name]['unit_to_market'][i] -
                               self.unit_commitment_vars[unit_name]['unit_to_market'][i - 1] -
                               max_ramp_up <= 0)
                self.model += (self.unit_commitment_vars[unit_name]['unit_to_market'][i - 1] -
                               self.unit_commitment_vars[unit_name]['unit_to_market'][i] -
                               max_ramp_down <= 0)

    def add_startup_costs(self, unit_name, hot_start_cost, cold_start_cost, time_to_go_cold):
        time_to_go_cold = self._minutes_to_intervals_round_down(time_to_go_cold)
        min_down_time = self._minutes_to_intervals_round_down(self.unit_min_down_time[unit_name])
        self._add_start_up_costs(unit_name, hot_start_cost, cold_start_cost, time_to_go_cold, min_down_time)

    def _add_start_up_costs(self, unit_name, hot_start_cost, cold_start_cost, time_to_go_cold, min_down_time):
        self.unit_commitment_vars[unit_name]['down_time_arc'] = {}
        cost_diff = (hot_start_cost - cold_start_cost)
        for i in range(0, self.planning_horizon):
            self.unit_commitment_vars[unit_name]['down_time_arc'][i] = {}
            for j in range(i + min_down_time, i + time_to_go_cold):
                self.unit_commitment_vars[unit_name]['down_time_arc'][i][j] = self.model.add_var(var_type=BINARY,
                                                                                                 obj=cost_diff)

        for i in range(0, self.planning_horizon):
            arc_vars = []
            for j in range(i - time_to_go_cold + 1, i - min_down_time):
                if (i in self.unit_commitment_vars[unit_name]['down_time_arc'] and
                        j in self.unit_commitment_vars[unit_name]['down_time_arc'][i]):
                    arc_vars.append(self.unit_commitment_vars[unit_name]['down_time_arc'][i][j])
            self.model += xsum(arc_vars) - self.unit_commitment_vars[unit_name]['startup_status'][i] <= 0

            arc_vars = []
            for j in range(i + min_down_time, i + time_to_go_cold - 1):
                if (j in self.unit_commitment_vars[unit_name]['down_time_arc'] and
                        i in self.unit_commitment_vars[unit_name]['down_time_arc'][j]):
                    arc_vars.append(self.unit_commitment_vars[unit_name]['down_time_arc'][j][i])
            self.model += xsum(arc_vars) - self.unit_commitment_vars[unit_name]['shutdown_status'][i] <= 0

            arc_vars = []
            for j in range(i - time_to_go_cold + 1, i - min_down_time):
                if (i in self.unit_commitment_vars[unit_name]['down_time_arc'] and
                        j in self.unit_commitment_vars[unit_name]['down_time_arc'][i]):
                    arc_vars.append(self.unit_commitment_vars[unit_name]['down_time_arc'][i][j])

            self.unit_commitment_vars[unit_name]['startup_status'][i].obj = cold_start_cost

    def add_market_to_unit_flow(self, unit_name, capacity):
        for i in range(0, self.planning_horizon):
            var_name = "{}_market_to_unit_{}".format(unit_name, i)
            self.unit_in_flow_variables[unit_name][i]['market_to_unit'] = self.model.add_var(name=var_name, ub=capacity)

    def add_regulation_service_to_output(self, unit_name, service, availability, ramp_rate, fcas_trapezium=None):
        capacity = self.unit_out_flow_variables[unit_name][0]['unit_to_market'].ub
        self.unit_output_fcas_variables[unit_name][service] = {}
        for i in range(0, self.planning_horizon):
            var_name = "{}_output_{}_{}".format(unit_name, service, i)
            self.unit_output_fcas_variables[unit_name][service][i] = self.model.add_var(name=var_name, ub=availability)

        self.add_joint_ramping_constraints_to_output(unit_name, service, ramp_rate)

        if fcas_trapezium is not None:
            self.add_capacity_constraints_on_output(unit_name=unit_name, service=service,
                                                    max_available=availability,
                                                    enablement_min=fcas_trapezium['enablement_min'],
                                                    low_breakpoint=fcas_trapezium['low_breakpoint'],
                                                    high_breakpoint=fcas_trapezium['high_breakpoint'],
                                                    enablement_max=fcas_trapezium['enablement_max'])

        elif 'raise' in service:
            self.add_capacity_constraints_on_output(unit_name=unit_name, service=service,
                                                    max_available=availability, enablement_min=0.0,
                                                    low_breakpoint=0.0, high_breakpoint=capacity - availability,
                                                    enablement_max=capacity)
        elif 'lower' in service:
            self.add_capacity_constraints_on_output(unit_name=unit_name, service=service,
                                                    max_available=availability, enablement_min=0.0,
                                                    low_breakpoint=availability, high_breakpoint=capacity,
                                                    enablement_max=capacity)

    def add_joint_ramping_constraints_to_output(self, unit_name, service, ramp_rate):

        for i in range(0, self.planning_horizon):

            if i == 0:
                previous_energy_dispatch_target = self.unit_initial_mw[unit_name]
            else:
                previous_energy_dispatch_target = self.unit_out_flow_variables[unit_name][i - 1]["unit_to_market"]

            energy_dispatch_target = self.unit_out_flow_variables[unit_name][i]["unit_to_market"]
            fcas_regulation_target = self.unit_output_fcas_variables[unit_name][service][i]

            if 'raise' in service:
                self.model += energy_dispatch_target + fcas_regulation_target - previous_energy_dispatch_target \
                              <= ramp_rate * (self.dispatch_interval / 60)

            elif 'lower' in service:
                self.model += energy_dispatch_target - fcas_regulation_target - previous_energy_dispatch_target \
                              >= - ramp_rate * (self.dispatch_interval / 60)

    def add_capacity_constraints_on_output(self, unit_name, service, max_available, enablement_min,
                                           low_breakpoint, high_breakpoint, enablement_max):

        if unit_name in self.unit_out_flow_variables:
            upper_slope_coefficient = (enablement_max - high_breakpoint) / max_available
            lower_slope_coefficient = (low_breakpoint - enablement_min) / max_available

            for i in range(0, self.planning_horizon):
                energy_dispatch_target = self.unit_out_flow_variables[unit_name][i]["unit_to_market"]
                fcas_contingency_target = self.unit_output_fcas_variables[unit_name][service][i]

                self.model += energy_dispatch_target + upper_slope_coefficient * fcas_contingency_target \
                              <= enablement_max

                self.model += energy_dispatch_target - lower_slope_coefficient * fcas_contingency_target \
                              >= enablement_min

    def add_regulation_service_to_input(self, unit_name, service, availability, ramp_rate, fcas_trapezium=None):
        capacity = self.unit_in_flow_variables[unit_name][0]['market_to_unit'].ub
        self.unit_input_fcas_variables[unit_name][service] = {}
        for i in range(0, self.planning_horizon):
            var_name = "{}_input_{}_{}".format(unit_name, service, i)
            self.unit_input_fcas_variables[unit_name][service][i] = self.model.add_var(name=var_name, ub=availability)

        self.add_joint_ramping_constraints_to_input(unit_name, service, ramp_rate)

        if fcas_trapezium is not None:
            self.add_capacity_constraints_on_input(unit_name=unit_name, service=service,
                                                   max_available=availability,
                                                   enablement_min=fcas_trapezium['enablement_min'],
                                                   low_breakpoint=fcas_trapezium['low_breakpoint'],
                                                   high_breakpoint=fcas_trapezium['high_breakpoint'],
                                                   enablement_max=fcas_trapezium['enablement_max'])

        elif 'raise' in service:
            self.add_capacity_constraints_on_input(unit_name=unit_name, service=service,
                                                   max_available=availability, enablement_min=0.0,
                                                   low_breakpoint=availability, high_breakpoint=capacity,
                                                   enablement_max=capacity)
        elif 'lower' in service:
            self.add_capacity_constraints_on_input(unit_name=unit_name, service=service,
                                                   max_available=availability, enablement_min=0.0,
                                                   low_breakpoint=0.0, high_breakpoint=capacity - availability,
                                                   enablement_max=capacity)

    def add_capacity_constraints_on_input(self, unit_name, service, max_available, enablement_min,
                                          low_breakpoint, high_breakpoint, enablement_max):

        if unit_name in self.unit_in_flow_variables:
            upper_slope_coefficient = (enablement_max - high_breakpoint) / max_available
            lower_slope_coefficient = (low_breakpoint - enablement_min) / max_available

            for i in range(0, self.planning_horizon):
                energy_dispatch_target = self.unit_in_flow_variables[unit_name][i]["market_to_unit"]
                fcas_contingency_target = self.unit_input_fcas_variables[unit_name][service][i]

                self.model += energy_dispatch_target + upper_slope_coefficient * fcas_contingency_target \
                              <= enablement_max

                self.model += energy_dispatch_target - lower_slope_coefficient * fcas_contingency_target \
                              >= enablement_min

    def add_joint_ramping_constraints_to_input(self, unit_name, service, ramp_rate):

        for i in range(0, self.planning_horizon):

            if i == 0:
                previous_energy_dispatch_target = self.unit_initial_mw[unit_name]
            else:
                previous_energy_dispatch_target = self.unit_in_flow_variables[unit_name][i - 1]["market_to_unit"]

            energy_dispatch_target = self.unit_in_flow_variables[unit_name][i]["market_to_unit"]
            fcas_regulation_target = self.unit_input_fcas_variables[unit_name][service][i]

            if 'raise' in service:
                self.model += energy_dispatch_target - fcas_regulation_target - previous_energy_dispatch_target \
                              >= - ramp_rate * (self.dispatch_interval / 60)

            elif 'lower' in service:
                self.model += energy_dispatch_target + fcas_regulation_target - previous_energy_dispatch_target \
                              <= ramp_rate * (self.dispatch_interval / 60)

    def add_contingency_service_to_output(self, unit_name, service, availability, fcas_trapezium=None):
        capacity = self.unit_out_flow_variables[unit_name][0]['unit_to_market'].ub
        self.unit_output_fcas_variables[unit_name][service] = {}
        for i in range(0, self.planning_horizon):
            var_name = "{}_output_{}_{}".format(unit_name, service, i)
            self.unit_output_fcas_variables[unit_name][service][i] = self.model.add_var(name=var_name, ub=availability)

        if fcas_trapezium is not None:
            self.add_joint_capacity_constraints_on_output(unit_name=unit_name, service=service,
                                                          max_available=availability,
                                                          enablement_min=fcas_trapezium['enablement_min'],
                                                          low_breakpoint=fcas_trapezium['low_breakpoint'],
                                                          high_breakpoint=fcas_trapezium['high_breakpoint'],
                                                          enablement_max=fcas_trapezium['enablement_max'])
        elif 'raise' in service:
            self.add_joint_capacity_constraints_on_output(unit_name=unit_name, service=service,
                                                          max_available=availability, enablement_min=0.0,
                                                          low_breakpoint=0.0, high_breakpoint=capacity - availability,
                                                          enablement_max=capacity)
        elif 'lower' in service:
            self.add_joint_capacity_constraints_on_output(unit_name=unit_name, service=service,
                                                          max_available=availability, enablement_min=0.0,
                                                          low_breakpoint=availability, high_breakpoint=capacity,
                                                          enablement_max=capacity)

    def add_contingency_service_to_input(self, unit_name, service, availability, fcas_trapezium=None):
        capacity = self.unit_in_flow_variables[unit_name][0]['market_to_unit'].ub
        self.unit_input_fcas_variables[unit_name][service] = {}
        for i in range(0, self.planning_horizon):
            var_name = "{}_input_{}_{}".format(unit_name, service, i)
            self.unit_input_fcas_variables[unit_name][service][i] = self.model.add_var(name=var_name, ub=availability)

        if fcas_trapezium is not None:
            self.add_joint_capacity_constraints_on_input(unit_name=unit_name, service=service,
                                                         max_available=availability,
                                                         enablement_min=fcas_trapezium['enablement_min'],
                                                         low_breakpoint=fcas_trapezium['low_breakpoint'],
                                                         high_breakpoint=fcas_trapezium['high_breakpoint'],
                                                         enablement_max=fcas_trapezium['enablement_max'])
        elif 'raise' in service:
            self.add_joint_capacity_constraints_on_input(unit_name=unit_name, service=service,
                                                         max_available=availability, enablement_min=0.0,
                                                         low_breakpoint=availability, high_breakpoint=capacity,
                                                         enablement_max=capacity)
        elif 'lower' in service:
            self.add_joint_capacity_constraints_on_input(unit_name=unit_name, service=service,
                                                         max_available=availability, enablement_min=0.0,
                                                         low_breakpoint=0.0, high_breakpoint=capacity - availability,
                                                         enablement_max=capacity)

    def add_joint_capacity_constraints_on_output(self, unit_name, service, max_available, enablement_min,
                                                 low_breakpoint, high_breakpoint, enablement_max):

        if unit_name in self.unit_out_flow_variables:
            upper_slope_coefficient = (enablement_max - high_breakpoint) / max_available
            lower_slope_coefficient = (low_breakpoint - enablement_min) / max_available

            for i in range(0, self.planning_horizon):
                energy_dispatch_target = self.unit_out_flow_variables[unit_name][i]["unit_to_market"]
                fcas_contingency_target = self.unit_output_fcas_variables[unit_name][service][i]

                if 'raise_regulation' in self.unit_output_fcas_variables[unit_name]:
                    raise_regulation_target = self.unit_output_fcas_variables[unit_name]['raise_regulation'][i]
                    self.model += energy_dispatch_target + upper_slope_coefficient * fcas_contingency_target + \
                                  raise_regulation_target <= enablement_max
                else:
                    self.model += energy_dispatch_target + upper_slope_coefficient * fcas_contingency_target \
                                  <= enablement_max

                if 'lower_regulation' in self.unit_output_fcas_variables[unit_name]:
                    lower_regulation_target = self.unit_output_fcas_variables[unit_name]['lower_regulation'][i]
                    self.model += energy_dispatch_target - lower_slope_coefficient * fcas_contingency_target - \
                                  lower_regulation_target >= enablement_min
                else:
                    self.model += energy_dispatch_target - lower_slope_coefficient * fcas_contingency_target \
                                  >= enablement_min

    def add_joint_capacity_constraints_on_input(self, unit_name, service, max_available, enablement_min,
                                                low_breakpoint, high_breakpoint, enablement_max):

        if unit_name in self.unit_in_flow_variables:
            upper_slope_coefficient = (enablement_max - high_breakpoint) / max_available
            lower_slope_coefficient = (low_breakpoint - enablement_min) / max_available

            for i in range(0, self.planning_horizon):
                energy_dispatch_target = self.unit_in_flow_variables[unit_name][i]["market_to_unit"]
                fcas_contingency_target = self.unit_input_fcas_variables[unit_name][service][i]

                if 'lower_regulation' in self.unit_input_fcas_variables[unit_name]:
                    lower_regulation_target = self.unit_input_fcas_variables[unit_name]['lower_regulation'][i]
                    self.model += energy_dispatch_target + upper_slope_coefficient * fcas_contingency_target + \
                                  lower_regulation_target <= enablement_max
                else:
                    self.model += energy_dispatch_target + upper_slope_coefficient * fcas_contingency_target \
                                  <= enablement_max

                if 'raise_regulation' in self.unit_input_fcas_variables[unit_name]:
                    raise_regulation_target = self.unit_input_fcas_variables[unit_name]['raise_regulation'][i]
                    self.model += energy_dispatch_target - lower_slope_coefficient * fcas_contingency_target - \
                                  raise_regulation_target >= enablement_min
                else:
                    self.model += energy_dispatch_target - lower_slope_coefficient * fcas_contingency_target \
                                  >= enablement_min

    def add_storage(self, unit_name, mwh, initial_mwh, output_capacity, output_efficiency,
                    input_capacity, input_efficiency):

        self.units_with_storage.append(unit_name)
        self.unit_storage_mwh[unit_name] = mwh
        self.unit_storage_initial_mwh[unit_name] = initial_mwh
        self.unit_storage_input_capacity[unit_name] = input_capacity
        self.unit_storage_output_capacity[unit_name] = output_capacity
        self.unit_storage_input_efficiency[unit_name] = input_efficiency
        self.unit_storage_output_efficiency[unit_name] = output_efficiency
        self.unit_storage_level_variables[unit_name] = {}

        for i in range(0, self.planning_horizon):
            input_var_name = "{}_unit_to_storage_{}".format(unit_name, i)
            self.unit_out_flow_variables[unit_name][i]['unit_to_storage'] = self.model.add_var(name=input_var_name,
                                                                                               ub=input_capacity)

            output_var_name = "{}_storage_to_unit_{}".format(unit_name, i)
            self.unit_in_flow_variables[unit_name][i]['storage_to_unit'] = self.model.add_var(name=output_var_name,
                                                                                              ub=output_capacity)

            storage_var_name = "{}_storage_level_{}".format(unit_name, i)
            self.unit_storage_level_variables[unit_name][i] = self.model.add_var(name=storage_var_name, ub=mwh)

            input_to_storage = self.unit_out_flow_variables[unit_name][i]['unit_to_storage']
            output_from_storage = self.unit_in_flow_variables[unit_name][i]['storage_to_unit']
            storage_level = self.unit_storage_level_variables[unit_name][i]
            hours_per_interval = self.dispatch_interval / 60

            if i == 0:
                self.model += initial_mwh - (output_from_storage / output_efficiency) * hours_per_interval + \
                              (input_to_storage * input_efficiency) * hours_per_interval == storage_level
            else:
                previous_storage_level = self.unit_storage_level_variables[unit_name][i - 1]
                self.model += previous_storage_level - (output_from_storage / output_efficiency) * hours_per_interval + \
                              (input_to_storage * input_efficiency) * hours_per_interval == storage_level

    def add_generator(self, unit_name, capacity, cost=0.0):
        for i in range(0, self.planning_horizon):
            input_var_name = "{}_generator_to_unit_{}".format(unit_name, i)
            self.unit_in_flow_variables[unit_name][i]['generator_to_unit'] = self.model.add_var(name=input_var_name,
                                                                                                ub=capacity,
                                                                                                obj=-1 * cost)

    def add_load(self, unit_name, capacity, cost=0.0):
        for i in range(0, self.planning_horizon):
            input_var_name = "{}_unit_to_load_{}".format(unit_name, i)
            self.unit_out_flow_variables[unit_name][i]['unit_to_load'] = self.model.add_var(name=input_var_name,
                                                                                            ub=capacity,
                                                                                            obj=-1 * cost)

    def optimise(self):
        self._create_constraints_to_balance_grid_nodes()
        self._create_constraints_to_balance_unit_nodes()
        self.model.optimize()

    def cross_market_optimise(self):
        self.optimise()

        convergence_reached = False

        while not convergence_reached:
            for region_market in self.regional_markets:
                modified_forward_data = self.forward_data
                for region_demand_to_update in self.regional_markets:
                    if 'energy' in region_demand_to_update and region_demand_to_update != region_market:
                        region = region_demand_to_update.split('-')[0]
                        forward_dispatch = self._get_forward_dispatch_trace(region, 'energy', self.forward_data)
                        modified_forward_data = pd.merge(modified_forward_data, forward_dispatch, on='interval')
                        fleet_dispatch_in_region = self.get_market_dispatch(region_demand_to_update)
                        modified_forward_data = pd.merge(modified_forward_data, fleet_dispatch_in_region, on='interval')
                        modified_forward_data[region + '-demand'] = modified_forward_data[region + '-demand'] - \
                                                                    (modified_forward_data['dispatch'] -
                                                                     modified_forward_data[
                                                                         region + '-energy-fleet-dispatch'])
                        modified_forward_data = modified_forward_data.drop(columns='dispatch')
                self._update_price_forecast(region_market, forward_data=modified_forward_data)
            old_dispatch = self.get_dispatch()
            self.model.optimize()
            convergence_reached = self._check_convergence(old_dispatch)

    def _check_convergence(self, previous_dispatch):
        current_dispatch = self.get_dispatch()
        for col in current_dispatch.columns:
            difference = ((current_dispatch[col] - previous_dispatch[col]) / previous_dispatch[col]).abs().max()
            if difference > 0.05:
                return False
        return True

    def _create_constraints_to_balance_grid_nodes(self):
        for market in self.regional_markets:
            region, service = market.split('-')
            for i in range(0, self.planning_horizon):
                if service == 'energy':
                    out_flow_vars = [self.unit_out_flow_variables[unit_name][i]["unit_to_market"] for
                                     unit_name in self.units if (
                                             market == self.unit_energy_market_mapping[unit_name] and
                                             "unit_to_market" in self.unit_out_flow_variables[unit_name][i])]
                    in_flow_vars = [self.unit_in_flow_variables[unit_name][i]["market_to_unit"] for
                                    unit_name in self.units if (
                                            market == self.unit_energy_market_mapping[unit_name] and
                                            "market_to_unit" in self.unit_in_flow_variables[unit_name][i])]
                    min_loading_vars = []
                    for unit, min_loading in self.unit_min_loading.items():
                        min_loading_vars.append(self.unit_commitment_vars[unit]['state'][i] * min_loading * -1)

                    self.model += xsum([self.market_net_dispatch_variables[region][service][i]] + in_flow_vars +
                                       [-1 * var for var in out_flow_vars] + min_loading_vars) == 0.0
                else:
                    out_flow_vars = [self.unit_output_fcas_variables[unit_name][service][i] for
                                     unit_name in self.units if (
                                             region == self.unit_fcas_market_mapping[service][unit_name] and
                                             service in self.unit_output_fcas_variables[unit_name])]
                    in_flow_vars = [self.unit_input_fcas_variables[unit_name][service][i] for
                                    unit_name in self.units if (
                                            region == self.unit_fcas_market_mapping[service][unit_name] and
                                            service in self.unit_input_fcas_variables[unit_name])]
                    self.model += xsum([self.market_net_dispatch_variables[region][service][i]] +
                                       [-1 * var for var in in_flow_vars] +
                                       [-1 * var for var in out_flow_vars]) == 0.0

    def _create_constraints_to_balance_unit_nodes(self):
        for unit in self.units:
            for i in range(0, self.planning_horizon):
                in_flow_vars = [var for var_name, var in self.unit_in_flow_variables[unit][i].items()]
                out_flow_vars = [var for var_name, var in self.unit_out_flow_variables[unit][i].items()]
                if unit in self.unit_commitment_vars:
                    min_loading_var = [self.unit_commitment_vars[unit]['state'][i] * self.unit_min_loading[unit] * -1]
                    self.model += xsum(in_flow_vars + [-1 * var for var in out_flow_vars] + min_loading_var) == 0.0
                else:
                    self.model += xsum(in_flow_vars + [-1 * var for var in out_flow_vars]) == 0.0

    def get_unit_dispatch(self, unit_name):
        energy_flows = self.get_unit_energy_flows(unit_name)
        dispatch = energy_flows.loc[:, ['interval', 'net_dispatch']]
        return dispatch

    def get_unit_energy_flows(self, unit_name):
        energy_flows = self.forward_data.loc[:, ['interval']]

        if 'unit_to_market' in self.unit_out_flow_variables[unit_name][0]:
            energy_flows['unit_to_market'] = \
                energy_flows['interval'].apply(lambda x: self.unit_out_flow_variables[unit_name][x]['unit_to_market'].x)

        if 'market_to_unit' in self.unit_in_flow_variables[unit_name][0]:
            energy_flows['market_to_unit'] = \
                energy_flows['interval'].apply(lambda x: self.unit_in_flow_variables[unit_name][x]['market_to_unit'].x)

        if 'generator_to_unit' in self.unit_in_flow_variables[unit_name][0]:
            energy_flows['generator_to_unit'] = \
                energy_flows['interval'].apply(lambda x: self.unit_in_flow_variables[unit_name][x]['generator_to_unit'].x)

        if 'state' in self.unit_commitment_vars[unit_name]:
            energy_flows['state'] = \
                energy_flows['interval'].apply(lambda x: self.unit_commitment_vars[unit_name]['state'][x].x)

        energy_flows['net_dispatch'] = 0.0

        if 'unit_to_market' in energy_flows.columns:
            energy_flows['net_dispatch'] += energy_flows['unit_to_market']

        if 'market_to_unit' in energy_flows.columns:
            energy_flows['net_dispatch'] -= energy_flows['market_to_unit']

        if 'state' in energy_flows.columns:
            energy_flows['net_dispatch'] += energy_flows['state'] * self.unit_min_loading[unit_name]

        return energy_flows

    def get_storage_energy_flows_and_state_of_charge(self, unit_name):
        if unit_name not in self.units_with_storage:
            raise ValueError('The unit specified does not have a storage component.')

        energy_flows = self.price_traces_by_market[self.regional_markets[0]].loc[:, ['interval']]

    def get_dispatch(self):
        trace = self.forward_data.loc[:, ['interval']]
        for market in self.regional_markets:
            trace[market + '-dispatch'] = \
                trace['interval'].apply(lambda x: self.model.var_by_name(str("net_dispatch_{}_{}".format(market, x))).x,
                                        self.model)
        return trace

    def get_market_dispatch(self, market):
        trace = self.forward_data.loc[:, ['interval']]
        trace['dispatch'] = \
            trace['interval'].apply(lambda x: self.model.var_by_name(str("net_dispatch_{}_{}".format(market, x))).x,
                                    self.model)
        return trace


def _create_dispatch_dependent_price_traces(price_forecast, self_dispatch_forecast, capacity_min, capacity_max,
                                            demand_increment):
    """

    Examples
    --------
    >>> price_forecast = pd.DataFrame({
    ...    'interval': [1, 2, 3, 4],
    ...    -30: [100.0, 200.0, 250.0, 500.0],
    ...    -20: [100.0, 200.0, 250.0, 500.0],
    ...    -10: [100.0, 200.0, 250.0, 500.0],
    ...     0:  [100.0, 200.0, 250.0, 500.0],
    ...     10:  [100.0, 200.0, 250.0, 500.0],
    ...     20: [40.0, 80.0, 250.0, 500.0],
    ...     30: [40.0, 80.0, 250.0, 500.0]
    ...     })

    >>> self_dispatch_forecast = pd.DataFrame({
    ...    'interval': [1, 2, 3, 4],
    ...    'dispatch':  [0.0, 10.0, 0.0, -10.0],
    ...    })

    >>> _create_dispatch_dependent_price_traces(price_forecast, self_dispatch_forecast, 0.0, 20.0, 10.0)
       interval   20.0   10.0    0.0
    0         1  100.0  100.0  100.0
    1         2  200.0  200.0  200.0
    2         3  250.0  250.0  250.0
    3         4  500.0  500.0  500.0

    Parameters
    ----------
    sample

    Returns
    -------

    """
    rows = []
    for i in range(0, len(price_forecast['interval'])):
        row = _process_row(price_forecast.iloc[i:i + 1, :], self_dispatch_forecast.iloc[i:i + 1, :], capacity_min,
                           capacity_max, demand_increment)
        rows.append(row)
    return pd.concat(rows)


def _process_row(price_forecast, self_dispatch_forecast, capacity_min, capacity_max, demand_increment):
    """

    Examples
    --------
    >>> price_forecast = pd.DataFrame({
    ...    'interval': [1],
    ...    -20: [100.0],
    ...    -10: [100.0],
    ...     0:  [100.0],
    ...     10:  [100.0],
    ...     20: [40.0]
    ...     })

    >>> self_dispatch_forecast = pd.DataFrame({
    ...    'interval': [1],
    ...    'dispatch':  [10.0],
    ...    })

    >>> _process_row(price_forecast, self_dispatch_forecast, 0.0, 20.0, 10.0)

    Parameters
    ----------
    sample

    Returns
    -------

    """
    dispatch = self_dispatch_forecast['dispatch'].iloc[0]
    demand_deltas = [col for col in price_forecast.columns if col != 'interval']

    # Transform the demand delta columns into absolute dispatch values.
    price_forecast.columns = ['interval'] + [-1.0 * demand_delta + dispatch for demand_delta in demand_deltas]
    dispatch_levels = [col for col in price_forecast.columns if col != 'interval']
    cols_to_keep = ['interval'] + [col for col in dispatch_levels if
                                   capacity_min - demand_increment < col < capacity_max + demand_increment]
    price_forecast = price_forecast.loc[:, cols_to_keep]
    return price_forecast


class Forecaster:
    def __init__(self, tabu_child_nodes=['hour', 'weekday', 'month'],
                 tabu_edges=[('constraint', 'demand'), ('demand', 'demand'),
                             ('constraint', 'constraint'), ('capacity', 'capacity'),
                             ('capacity', 'demand'), ('demand', 'capacity')]):
        self.generic_tabu_child_nodes = tabu_child_nodes
        self.generic_tabu_edges = tabu_edges

    def _expand_tabu_edges(self, data_columns):
        """Prepare the tabu_edges input for the DAGregressor

        Examples
        --------

        >>> f = Forecaster()

        >>> f._expand_tabu_edges(data_columns=['demand-1', 'demand-2', 'constraint-1',
        ...                                    'availability-1', 'availability-2'])

        Parameters
        ----------
        data_columns

        Returns
        -------

        """
        expanded_edges = []
        for generic_edge in self.generic_tabu_edges:
            first_generic_node = generic_edge[0]
            second_generic_node = generic_edge[1]
            specific_first_nodes = [col for col in data_columns if first_generic_node in col]
            specific_second_nodes = [col for col in data_columns if second_generic_node in col]
            specific_edges = product(specific_first_nodes, specific_second_nodes)
            specific_edges = [edge for edge in specific_edges if edge[0] != edge[1]]
            expanded_edges += specific_edges

        return expanded_edges

    def train(self, data, train_sample_fraction, target_col):
        self.target_col = target_col
        self.features = [col for col in data.columns
                         if col not in [target_col, 'interval'] and 'fleet-dispatch' not in col]
        tabu_child_nodes = [col for col in self.generic_tabu_edges if col in self.features]
        self.regressor = DAGRegressor(threshold=0.0,
                                      alpha=0.0,
                                      beta=0.5,
                                      fit_intercept=True,
                                      hidden_layer_units=[5],
                                      standardize=True,
                                      tabu_child_nodes=tabu_child_nodes,
                                      tabu_edges=self._expand_tabu_edges(self.features))
        n_rows = len(data.index)
        sample_size = int(n_rows * train_sample_fraction)
        train = data.sample(sample_size, random_state=1)
        train = train.reset_index(drop=True)
        X, y = train.loc[:, self.features], np.asarray(train[target_col])
        self.regressor.fit(X, y)

    def price_forecast(self, forward_data, region, market, min_delta, max_delta, steps):
        prediction = forward_data.loc[:, ['interval']]
        forward_data['old_demand'] = forward_data[region + '-demand'] + forward_data[market + '-fleet-dispatch']
        delta_step_size = max(int((max_delta - min_delta) / steps), 1)
        for delta in range(int(min_delta), int(max_delta) + delta_step_size * 2, delta_step_size):
            forward_data[region + '-demand'] = forward_data['old_demand'] - delta
            X = forward_data.loc[:, self.features]
            Y = self.regressor.predict(X)
            prediction[delta] = Y
        return prediction

    def base_forecast(self, forward_data):
        prediction = forward_data.loc[:, ['interval']]
        X = forward_data.loc[:, self.features]
        Y = self.regressor.predict(X)
        prediction[self.target_col] = Y
        return prediction
