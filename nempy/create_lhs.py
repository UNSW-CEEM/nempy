import pandas as pd


def create(constraints, decision_variables, join_columns):
    constraints = pd.concat(list(constraints.values()))
    decision_variables = pd.concat(list(decision_variables.values()))
    constraints = pd.merge(constraints, decision_variables, 'inner', on=join_columns)
    constraints['coefficient'] = constraints['coefficient_x'] * constraints['coefficient_y']
    constraints = constraints.loc[:, ['constraint_id', 'variable_id', 'coefficient']]
    return constraints
