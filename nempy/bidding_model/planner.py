import pandas as pd
from mip import Model, xsum, maximize, INF, BINARY


class DispatchPlanner:
    def __init__(self, dispatch_interval):
        self.dispatch_interval = dispatch_interval
        self.planning_horizon = None
        self.market_nodes = []
        self.price_traces_by_node = {}
        self.units = []
        self.unit_node_mapping = {}
        self.model = Model(solver_name='CBC')
        self.unit_in_flow_variables = {}
        self.unit_out_flow_variables = {}
        self.unit_storage_input_capacity = {}
        self.unit_storage_output_capacity = {}
        self.unit_storage_input_efficiency = {}
        self.unit_storage_output_efficiency = {}
        self.unit_storage_mwh = {}
        self.unit_storage_initial_mwh = {}
        self.unit_storage_level_variables = {}
        self.market_node_dispatch_variables = {}
        self.market_node_revenue_variables = {}
        self.market_node_dispatch_revenue_interpolation_weights = {}

    def add_market_node(self, name, price_traces):
        self.market_nodes.append(name)
        self.price_traces_by_node[name] = price_traces
        self.planning_horizon = len(price_traces['interval'])
        revenue_traces = self._get_revenue_traces(name)
        self.market_node_dispatch_variables[name] = {}
        self.market_node_revenue_variables[name] = {}
        self.market_node_dispatch_revenue_interpolation_weights[name] = {}
        for i in range(0, self.planning_horizon):
            dispatch_var_name = "dispatch_{}".format(i)
            self.market_node_dispatch_variables[name][i] = self.model.add_var(name=dispatch_var_name, lb=-1 * INF, ub=INF)
            revenue_var_name = "revenue_{}".format(i)
            self.market_node_revenue_variables[name][i] = self.model.add_var(name=revenue_var_name, lb=-1 * INF, ub=INF)
            self.market_node_dispatch_revenue_interpolation_weights[name][i] = {}
            trace_dispatch_levels = [level for level in revenue_traces.columns if level != 'interval']
            for level in trace_dispatch_levels:
                interpolation_var_name = "interpolation_{}_{}".format(i, level)
                self.market_node_dispatch_revenue_interpolation_weights[name][i][level] = self.model.add_var(interpolation_var_name,
                                                                                           lb=0.0, ub=1.0)
            self.model += xsum(self.market_node_dispatch_revenue_interpolation_weights[name][i].values()) == 1.0
            weights = self.market_node_dispatch_revenue_interpolation_weights[name][i]
            revenue = revenue_traces.loc[i].to_dict()
            self.model += xsum([level * weight_var for level, weight_var in weights.items()]) == \
                          self.market_node_dispatch_variables[name][i]
            self.model += xsum([revenue[level] * weight_var for level, weight_var in weights.items()]) == \
                          self.market_node_revenue_variables[name][i]
            self.model.add_sos([(weight_var, level) for level, weight_var in weights.items()], 2)

    def add_unit(self, name, market_node_name):
        self.units.append(name)
        self.unit_node_mapping[name] = market_node_name
        self.unit_in_flow_variables[name] = {}
        self.unit_out_flow_variables[name] = {}

        for i in range(0, self.planning_horizon):
            self.unit_in_flow_variables[name][i] = {}
            self.unit_out_flow_variables[name][i] = {}

    def add_unit_to_market_flow(self, unit_name, capacity):
        for i in range(0, self.planning_horizon):
            var_name = "{}_unit_to_market_{}".format(unit_name, i)
            self.unit_out_flow_variables[unit_name][i]['unit_to_market'] = self.model.add_var(name=var_name,
                                                                                              ub=capacity)

    def add_market_to_unit_flow(self, unit_name, capacity):
        for i in range(0, self.planning_horizon):
            var_name = "{}_market_to_unit_{}".format(unit_name, i)
            self.unit_in_flow_variables[unit_name][i]['market_to_unit'] = self.model.add_var(name=var_name, ub=capacity)

    def add_storage(self, unit_name, mwh, initial_mwh, output_capacity, output_efficiency,
                    input_capacity, input_efficiency):

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

    def _get_revenue_traces(self, name):
        revenue_traces = self.price_traces_by_node[name]
        for col in revenue_traces.columns:
            if col != 'interval':
                revenue_traces[col] = revenue_traces[col] * (col + 0.00001)
        return revenue_traces

    def optimise(self):
        self._create_constraints_to_balance_grid_nodes()
        self._create_constraints_to_balance_unit_nodes()
        self._add_objective_function()
        self.model.optimize()

    def _create_constraints_to_balance_grid_nodes(self):
        for node in self.market_nodes:
            for i in range(0, self.planning_horizon):
                out_flow_vars = [self.unit_out_flow_variables[unit_name][i]["unit_to_market"] for
                                unit_name in self.units if node == self.unit_node_mapping[unit_name]]
                in_flow_vars = [self.unit_in_flow_variables[unit_name][i]["market_to_unit"] for
                                 unit_name in self.units if node == self.unit_node_mapping[unit_name]]
                self.model += xsum([self.market_node_dispatch_variables[node][i]] + in_flow_vars +
                                   [-1 * var for var in out_flow_vars]) == 0.0

    def _create_constraints_to_balance_unit_nodes(self):
        for unit in self.units:
            for i in range(0, self.planning_horizon):
                in_flow_vars = [var for var_name, var in self.unit_in_flow_variables[unit][i].items()]
                out_flow_vars = [var for var_name, var in self.unit_out_flow_variables[unit][i].items()]
                self.model += xsum(in_flow_vars + [-1 * var for var in out_flow_vars]) == 0.0

    def _add_objective_function(self):
        revenue_vars = []
        for node in self.market_nodes:
            revenue_vars += [var for name, var in self.market_node_revenue_variables[node].items()]
        self.model.objective = maximize(xsum(revenue_vars))

    def get_dispatch(self):
        trace = self.price_traces_by_node[self.market_nodes[0]].loc[:, ['interval']]
        trace['dispatch'] = \
            trace['interval'].apply(lambda x: self.model.var_by_name(str("dispatch_{}".format(x))).x, self.model)
        trace['revenue'] = \
            trace['interval'].apply(lambda x: self.model.var_by_name(str("revenue_{}".format(x))).x, self.model)
        trace['unit_to_market'] = \
            trace['interval'].apply(lambda x: self.model.var_by_name(str("stor_unit_to_market_{}".format(x))).x, self.model)
        trace['market_to_unit'] = \
            trace['interval'].apply(lambda x: self.model.var_by_name(str("stor_market_to_unit_{}".format(x))).x, self.model)
        trace['unit_to_storage'] = \
            trace['interval'].apply(lambda x: self.model.var_by_name(str("stor_unit_to_storage_{}".format(x))).x, self.model)
        trace['storage_to_unit'] = \
            trace['interval'].apply(lambda x: self.model.var_by_name(str("stor_storage_to_unit_{}".format(x))).x, self.model)
        trace['storage'] = \
            trace['interval'].apply(lambda x: self.model.var_by_name(str("stor_storage_level_{}".format(x))).x, self.model)
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
        row = _process_row(price_forecast.iloc[i:i+1,:], self_dispatch_forecast.iloc[i:i+1,:],  capacity_min,
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
