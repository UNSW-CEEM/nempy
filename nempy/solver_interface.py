import numpy as np
import pandas as pd
from mip import Model, xsum, minimize, INTEGER, CONTINUOUS, OptimizationStatus, LinExpr, BINARY


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

    # 1. Create the decision variables
    decision_variables = pd.concat(decision_variables)
    lp_variables = {}
    variable_types = {'continuous': CONTINUOUS, 'binary': BINARY}
    sos_one_weights = []
    for variable_id, lower_bound, upper_bound, variable_type in zip(
            list(decision_variables['variable_id']), list(decision_variables['lower_bound']),
            list(decision_variables['upper_bound']), list(decision_variables['type'])):
        lp_variables[variable_id] = prob.add_var(lb=lower_bound, ub=upper_bound, var_type=variable_types[variable_type],
                                                 name=str(variable_id))

    # 2. Create the objective function
    objective_function = pd.concat(list(objective_function.values()))
    objective_function = objective_function.sort_values('variable_id')
    objective_function = objective_function.set_index('variable_id')
    prob.objective = minimize(xsum(objective_function['cost'][i] * lp_variables[i] for i in
                                   list(objective_function.index)))

    # 3. Create the constraints
    if len(constraints_rhs_and_type) > 0:
        sos_constraints = list(constraints_rhs_and_type['interpolation_weights']['constraint_id'])
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

    constraint_matrix = constraints_lhs.pivot('constraint_id', 'variable_id', 'coefficient')
    constraint_matrix = constraint_matrix.sort_index(axis=1)
    constraint_ids = np.asarray(constraint_matrix.index)
    constraint_matrix_np = np.asarray(constraint_matrix)

    rhs = dict(zip(rhs_and_type['constraint_id'], rhs_and_type['rhs']))
    enq_type = dict(zip(rhs_and_type['constraint_id'], rhs_and_type['type']))
    var_list = np.asarray([lp_variables[k] for k in sorted(list(lp_variables))])
    for row, row_index in zip(constraint_matrix_np, constraint_ids):
        new_constraint = make_constraint(var_list, row, rhs[row_index], enq_type[row_index], marginal_offset=0)
        prob.add_constr(new_constraint, name=str(row_index))

    for row_index in sos_constraints:
        sos_set = get_sos(var_list, constraint_matrix_np[row_index])
        prob.add_sos(list(zip(sos_set, [0 for var in sos_set])), 2)

    # 4. Solve the problem
    status = prob.optimize()
    if status != OptimizationStatus.OPTIMAL:
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

    # 6. Retrieve the shadow costs of market constraints
    for constraint_group in market_rhs_and_type.keys():
        market_rhs_and_type[constraint_group]['price'] = \
            market_rhs_and_type[constraint_group].apply(lambda x: get_price(x['constraint_id'], prob), axis=1)

    return split_decision_variables, market_rhs_and_type


def make_constraint(var_list, lhs, rhs, enq_type, marginal_offset=0):
    needed_variables_indices = np.argwhere(~np.isnan(lhs)).flatten()
    lhs_variables = var_list[needed_variables_indices]
    lhs = lhs[needed_variables_indices]
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


def get_sos(var_list, lhs):
    needed_variables_indices = np.argwhere(~np.isnan(lhs)).flatten()
    lhs_variables = var_list[needed_variables_indices]
    return lhs_variables


def get_price(row_index, prob):
    row_index = get_con_by_name(prob.constrs, str(row_index))
    constraint = prob.constrs[row_index]
    return constraint.pi


def get_con_by_name(constraints, name):
    i = 0
    for con in constraints:
        if con.name == name:
            return i
        i += 1