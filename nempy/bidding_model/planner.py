import pandas as pd
from mip import Model, xsum, maximize

from nempy.bidding_model import model_interface


class DispatchPlanner:
    def __init__(self, dispatch_interval, price_traces, min_capacity, max_capacity):
        self.dispatch_interval = dispatch_interval
        self.price_traces = price_traces
        self.model = Model()
        self.planning_horizon = len(price_traces['interval'])
        self.dispatch_variables = {}
        self.revenue_variables = {}
        self.dispatch_revenue_interpolation_weights = {}
        self.storage_level_variables = {}
        revenue_traces = self._get_revenue_traces()
        for i in range(0, self.planning_horizon):
            dispatch_var_name = "dispatch_{}".format(i)
            self.dispatch_variables[i] = self.model.add_var(name=dispatch_var_name, lb=min_capacity, ub=max_capacity)
            revenue_var_name = "revenue_{}".format(i)
            self.revenue_variables[i] = self.model.add_var(name=revenue_var_name, lb=min_capacity, ub=max_capacity)
            self.dispatch_revenue_interpolation_weights[i] = {}
            trace_dispatch_levels = [level for level in revenue_traces.columns if level != 'interval']
            for level in trace_dispatch_levels:
                interpolation_var_name = "interpolation_{}_{}".format(i, level)
                self.dispatch_revenue_interpolation_weights[i][level] = self.model.add_var(interpolation_var_name,
                                                                                           lb=0.0, ub=1.0)
            self.model += xsum(self.dispatch_revenue_interpolation_weights[i].values()) == 1.0
            weights = self.dispatch_revenue_interpolation_weights[i]
            revenue = revenue_traces.loc[i].to_dict()
            self.model += xsum([level * weight_var for level, weight_var in weights.items()]) == \
                          self.dispatch_variables[i]
            self.model += xsum([revenue[level] * weight_var for level, weight_var in weights.items()]) == \
                          self.revenue_variables[i]
            self.model.add_sos([(weight_var, level) for level, weight_var in weights.items()], 2)

        self.model.objective = maximize(xsum([var * 1.0 for name, var in self.revenue_variables.items()]))

    def add_storage_size(self, mwh, initial_mwh):
        for i in range(0, self.planning_horizon):
            storage_level_var_name = "storage_level_{}".format(i)
            self.storage_level_variables[i] = self.model.add_var(name=storage_level_var_name, lb=0.0, ub=mwh)
            if i == 0:
                self.model += initial_mwh - self.dispatch_variables[i] / 0.9 == self.storage_level_variables[i]
            else:
                self.model += self.storage_level_variables[i-1] - self.dispatch_variables[i] / 0.9 == \
                              self.storage_level_variables[i]

    def _get_revenue_traces(self):
        revenue_traces = self.price_traces
        for col in revenue_traces.columns:
            if col != 'interval':
                revenue_traces[col] = revenue_traces[col] * (col * 0.00001)
        return revenue_traces

    def optimise(self):
        self.model.optimize()

    def get_dispatch(self):
        self.price_traces['dispatch'] = \
            self.price_traces['interval'].apply(lambda x: self.model.var_by_name(str("dispatch_{}".format(x))).x, self.model)
        self.price_traces['storage'] = \
            self.price_traces['interval'].apply(lambda x: self.model.var_by_name(str("storage_level_{}".format(x))).x, self.model)
        return self.price_traces.loc[:, ['interval', 'dispatch', 'storage']]


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
