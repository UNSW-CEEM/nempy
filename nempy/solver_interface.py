import numpy as np
import pandas as pd
from mip import Model, xsum, minimize, INTEGER, CONTINUOUS, OptimizationStatus, LinExpr, BINARY
from nempy import check
from time import time


def dispatch(decision_variables, constraints_lhs, constraints_rhs_and_type, market_rhs_and_type,
             constraints_dynamic_rhs_and_type, objective_function):
    """Create and solve a linear program, returning prices of the market constraints and decision variables values.

    0. Create the problem instance as a mip-python object instance
    1. Create the decision variables
    2. Create the objective function
    3. Create the constraints
    4. Solve the problem
    5. Retrieve optimal values of each variable
    6. Retrieve the shadow costs of market constraints

    :param decision_variables: dict of DataFrames each with the following columns
        variable_id: int
        lower_bound: float
        upper_bound: float
        type: str one of 'continuous', 'integer' or 'binary'
    :param constraints_lhs_coefficient: dict of DataFrames each with the following columns
        variable_id: int
        constraint_id: int
        coefficient: float
    :param constraints_rhs_and_type: dict of DataFrames each with the following columns
        constraint_id: int
        type: str one of '=', '<=', '>='
        rhs: float
    :param market_constraints_lhs_coefficients: dict of DataFrames each with the following columns
        variable_id: int
        constraint_id: int
        coefficient: float
    :param market_rhs_and_type: dict of DataFrames each with the following columns
        constraint_id: int
        type: str one of '=', '<=', '>='
        rhs: float
    :param objective_function: dict of DataFrames each with the following columns
        variable_id: int
        cost: float
    :return:
        decision_variables: dict of DataFrames each with the following columns
            variable_id: int
            lower_bound: float
            upper_bound: float
            type: str one of 'continuous', 'integer' or 'binary'
            value: float
        market_rhs_and_type: dict of DataFrames each with the following columns
            constraint_id: int
            type: str one of '=', '<=', '>='
            rhs: float
            price: float
    """

    # 0. Create the problem instance as a mip-python object instance
    prob = Model("market")
    prob.verbose = 0

    sos_variables = None
    if 'interpolation_weights' in decision_variables.keys():
        sos_variables = decision_variables['interpolation_weights']

    # 1. Create the decision variables
    decision_variables = pd.concat(decision_variables)
    lp_variables = {}
    variable_types = {'continuous': CONTINUOUS, 'binary': BINARY}
    for variable_id, lower_bound, upper_bound, variable_type in zip(
            list(decision_variables['variable_id']), list(decision_variables['lower_bound']),
            list(decision_variables['upper_bound']), list(decision_variables['type'])):
        lp_variables[variable_id] = prob.add_var(lb=lower_bound, ub=upper_bound, var_type=variable_types[variable_type],
                                                 name=str(variable_id))

    def add_sos_vars(sos_group):
        prob.add_sos(list(zip(sos_group['vars'], sos_group['loss_segment'])), 2)

    if sos_variables is not None:
        sos_variables['vars'] = sos_variables['variable_id'].apply(lambda x: lp_variables[x])
        sos_variables.groupby('interconnector').apply(add_sos_vars)

    # 2. Create the objective function
    if len(objective_function) > 0:
        objective_function = pd.concat(list(objective_function.values()))
        objective_function = objective_function.sort_values('variable_id')
        objective_function = objective_function.set_index('variable_id')
        prob.objective = minimize(xsum(objective_function['cost'][i] * lp_variables[i] for i in
                                       list(objective_function.index)))

    # 3. Create the constraints
    constraints_rhs_and_type_original = constraints_rhs_and_type.copy()
    if len(constraints_rhs_and_type) > 0:
        constraints_rhs_and_type = pd.concat(list(constraints_rhs_and_type.values()))
    else:
        constraints_rhs_and_type = pd.DataFrame({})

    if len(constraints_dynamic_rhs_and_type) > 0:
        constraints_dynamic_rhs_and_type = pd.concat(list(constraints_dynamic_rhs_and_type.values()))
        constraints_dynamic_rhs_and_type['rhs'] = constraints_dynamic_rhs_and_type.\
            apply(lambda x: lp_variables[x['rhs_variable_id']], axis=1)
        rhs_and_type = pd.concat([constraints_rhs_and_type] + list(market_rhs_and_type.values()) +
                                 [constraints_dynamic_rhs_and_type])
    else:
        rhs_and_type = pd.concat([constraints_rhs_and_type] + list(market_rhs_and_type.values()))

    #constraint_matrix = constraints_lhs.pivot('constraint_id', 'variable_id', 'coefficient')
    constraint_matrix = pd.pivot_table(constraints_lhs, 'coefficient', 'constraint_id', 'variable_id', aggfunc='sum')
    constraint_matrix = constraint_matrix.sort_index(axis=1)
    constraint_matrix = constraint_matrix.sort_index()
    column_ids = np.asarray(constraint_matrix.columns)
    row_ids = np.asarray(constraint_matrix.index)
    constraint_matrix_np = np.asarray(constraint_matrix)

    # if len(constraint_matrix.columns) != max(decision_variables['variable_id']) + 1:
    #     raise check.ModelBuildError("Not all variables used in constraint matrix")

    rhs = dict(zip(rhs_and_type['constraint_id'], rhs_and_type['rhs']))
    enq_type = dict(zip(rhs_and_type['constraint_id'], rhs_and_type['type']))
    #var_list = np.asarray([lp_variables[k] for k in sorted(list(lp_variables))])
    #t0 = time()
    for row, id in zip(constraint_matrix_np, row_ids):
        new_constraint = make_constraint(lp_variables, row, rhs[id], column_ids, enq_type[id])
        prob.add_constr(new_constraint, name=str(id))
    #print(time() - t0)
    # for row_index in sos_constraints:
    #     sos_set = get_sos(var_list, constraint_matrix_np[row_index], column_ids)
     #   prob.add_sos(list(zip(sos_set, [0 for var in sos_set])), 1)

    # 4. Solve the problem
    status = prob.optimize()
    if status != OptimizationStatus.OPTIMAL:
        # Attempt find constraint causing infeasibility.
        con_index = find_problem_constraint(prob)
        print('Couldn\'t find an optimal solution, but removing con {} fixed INFEASIBLITY'.format(con_index))
        raise ValueError('Linear program infeasible')

    # 5. Retrieve optimal values of each variable
    decision_variables = decision_variables.droplevel(1)
    decision_variables['lp_variables'] = [lp_variables[i] for i in decision_variables['variable_id']]
    decision_variables['value'] = decision_variables['lp_variables'].apply(lambda x: x.x)
    decision_variables = decision_variables.drop('lp_variables', axis=1)
    split_decision_variables = {}
    for variable_group in decision_variables.index.unique():
        split_decision_variables[variable_group] = \
            decision_variables[decision_variables.index == variable_group].reset_index(drop=True)

    for constraint_group in constraints_rhs_and_type_original.keys():
        constraints_rhs_and_type_original[constraint_group]['slack'] = \
            constraints_rhs_and_type_original[constraint_group]['constraint_id']. \
        apply(lambda x: prob.constr_by_name(str(x)).slack, prob)

    #print('get values {}'.format(time() - t0))

    # 6. Retrieve the shadow costs of market constraints
    start_obj = prob.objective.x
    #initial_solution = [(v, v.x) for v in list(sos_variables['vars']) if v.x > 0.01]
    #print(initial_solution)
    #prob.start = initial_solution
    #prob.validate_mip_start()
    for constraint_group in market_rhs_and_type.keys():
        cg = constraint_group
        market_rhs_and_type[cg]['price'] = 0.0
        for id in list(market_rhs_and_type[cg]['constraint_id']):
            constraint = prob.constr_by_name(str(id))
            constraint.rhs += 1.0
            #t0 = time()
            prob.optimize()
            #tc += time() - t0
            marginal_cost = prob.objective.x - start_obj
            market_rhs_and_type[cg].loc[market_rhs_and_type[cg]['constraint_id'] == id, 'price'] = marginal_cost
            constraint.rhs -= 1.0
        # market_rhs_and_type[constraint_group]['price'] = \
        #     market_rhs_and_type[constraint_group].apply(lambda x: get_price(x['constraint_id'], prob), axis=1)
    #print(tc)
    return split_decision_variables, market_rhs_and_type, constraints_rhs_and_type_original


def make_constraint(lp_variables, lhs, rhs, column_ids, enq_type, marginal_offset=0):
    columns_in_constraint = np.argwhere(~np.isnan(lhs)).flatten()
    column_ids_in_constraint = column_ids[columns_in_constraint]
    # lhs_variables = lp_variables[column_ids_in_constraint]
    lhs_variables = np.asarray([lp_variables[k] for k in column_ids_in_constraint])
    lhs = lhs[columns_in_constraint]
    exp = lhs_variables * lhs
    exp = exp.tolist()
    exp = xsum(exp)
    # Add based on inequality type.
    if enq_type == '<=':
        con = exp <= rhs + marginal_offset
    elif enq_type == '>=':
        con = exp >= rhs + marginal_offset
    elif enq_type == '=':
        con = exp == rhs + marginal_offset
    else:
        print('missing types')
    return con


def find_problem_constraint(base_prob):
    cons = []
    test_prob = base_prob.copy()
    for con in [con.name for con in base_prob.constrs]:
        [test_prob.remove(c) for c in test_prob.constrs if c.name == con]
        status = test_prob.optimize()
        cons.append(con)
        if status == OptimizationStatus.OPTIMAL:
            return cons
    return []


class InterfaceToSolver:
    def __init__(self):
        self.variable_definitions = None
        self.variables = None
        self.constraints_type_and_rhs = None
        self.prob = Model("market")
        self.prob.verbose = 0

    def add_variables(self, decision_variables):
        self.variable_definitions = decision_variables
        variable_types = {'continuous': CONTINUOUS, 'binary': BINARY}
        for variable_id, lower_bound, upper_bound, variable_type in zip(
                list(decision_variables['variable_id']), list(decision_variables['lower_bound']),
                list(decision_variables['upper_bound']), list(decision_variables['type'])):
            self.variables[variable_id] = self.prob.add_var(lb=lower_bound, ub=upper_bound,
                                                            var_type=variable_types[variable_type],
                                                            name=str(variable_id))

    def add_sos_type_2(self, sos_variables):

        def add_sos_vars(sos_group):
            self.prob.add_sos(list(zip(sos_group['vars'], sos_group['loss_segment'])), 2)

        if sos_variables is not None:
            sos_variables['vars'] = sos_variables['variable_id'].apply(lambda x: self.variables[x])
            sos_variables.groupby('interconnector').apply(add_sos_vars)

    def add_objective_function(self, objective_function):
        objective_function = pd.concat(list(objective_function.values()))
        objective_function = objective_function.sort_values('variable_id')
        objective_function = objective_function.set_index('variable_id')
        self.prob.objective = minimize(xsum(objective_function['cost'][i] * self.variables[i] for i in
                                       list(objective_function.index)))

    def add_constraints(self, constraints_lhs, constraints_type_and_rhs):
        self.constraints_type_and_rhs = constraints_type_and_rhs
        constraint_matrix = pd.pivot_table(constraints_lhs, 'coefficient', 'constraint_id', 'variable_id',
                                           aggfunc='sum')
        constraint_matrix = constraint_matrix.sort_index(axis=1)
        constraint_matrix = constraint_matrix.sort_index()
        column_ids = np.asarray(constraint_matrix.columns)
        row_ids = np.asarray(constraint_matrix.index)
        constraint_matrix_np = np.asarray(constraint_matrix)

        rhs = dict(zip(constraints_type_and_rhs['constraint_id'], constraints_type_and_rhs['rhs']))
        enq_type = dict(zip(constraints_type_and_rhs['constraint_id'], constraints_type_and_rhs['type']))
        for row, id in zip(constraint_matrix_np, row_ids):
            new_constraint = make_constraint(self.variables, row, rhs[id], column_ids, enq_type[id])
            self.prob.add_constr(new_constraint, name=str(id))

    def optimize(self):
        status = self.prob.optimize()
        if status != OptimizationStatus.OPTIMAL:
            # Attempt find constraint causing infeasibility.
            con_index = find_problem_constraint(self.prob)
            print('Couldn\'t find an optimal solution, but removing con {} fixed INFEASIBLITY'.format(con_index))
            raise ValueError('Linear program infeasible')

    def get_optimal_values_of_decision_variables(self):
        self.variable_definitions['variables'] = [self.variables[i] for i in self.variable_definitions['variable_id']]
        self.variable_definitions['value'] = self.variable_definitions['variables'].apply(lambda x: x.x)
        self.variable_definitions = self.variable_definitions.drop('variables', axis=1)
        return self.variable_definitions

    def get_slack_in_constraints(self):
        self.constraints_type_and_rhs['slack'] = \
            self.constraints_type_and_rhs['constraint_id'].apply(lambda x: self.prob.constr_by_name(str(x)).slack,
                                                                 self.prob)

    def price_constraints(self, constraint_ids_to_price):
        start_obj = self.prob.objective.x
        costs = {}
        for id in constraint_ids_to_price:
            constraint = self.prob.constr_by_name(str(id))
            constraint.rhs += 1.0
            self.prob.optimize()
            marginal_cost = self.prob.objective.x - start_obj
            constraint.rhs -= 1.0
            costs[id] = marginal_cost
        return costs


def create_mapping_of_generic_constraint_sets_to_constraint_ids(constraints, market_constraints):
    generic_rhs = []
    if 'generic' in constraints:
        generic_rhs.append(constraints['generic'].loc[:, ['constraint_id', 'set']])
    if 'fcas' in market_constraints:
        generic_rhs.append(market_constraints['fcas'].loc[:, ['constraint_id', 'set']])
    if len(generic_rhs) > 0:
        return pd.concat(generic_rhs)
    else:
        return None


def create_unit_level_generic_constraint_lhs(generic_constraint_units, generic_constraint_ids,
                                             unit_bids_to_constraint_map):
    unit_lhs = pd.merge(generic_constraint_units,
                        unit_bids_to_constraint_map.loc[:, ['unit', 'service', 'variable_id']],
                        on=['unit', 'service'])
    unit_lhs = pd.merge(unit_lhs, generic_constraint_ids.loc[:, ['constraint_id', 'set']], on='set')
    return unit_lhs


def create_region_level_generic_constraint_lhs(generic_constraint_regions, generic_constraint_ids,
                                               regional_bids_to_constraint_map):
    region_lhs = pd.merge(generic_constraint_regions,
                          regional_bids_to_constraint_map.loc[:, ['region', 'service', 'variable_id']],
                          on=['region', 'service'])
    region_lhs = pd.merge(region_lhs, generic_constraint_ids.loc[:, ['constraint_id', 'set']], on='set')
    return region_lhs


def create_interconnector_generic_constraint_lhs(generic_constraint_interconnectors, generic_constraint_ids,
                                                 interconnector_variables):
    interconnector_lhs = pd.merge(generic_constraint_interconnectors,
                                  interconnector_variables.loc[:, ['interconnector', 'variable_id']],
                                  on=['interconnector'])
    interconnector_lhs = pd.merge(interconnector_lhs, generic_constraint_ids.loc[:, ['constraint_id', 'set']], on='set')
    return interconnector_lhs
