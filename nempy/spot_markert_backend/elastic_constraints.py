import pandas as pd
import numpy as np
from nempy.help_functions import helper_functions as hf


def create_deficit_variables(constraint_rhs, next_variable_id):
    """ Create variables that allow a constraint to violated at a specified cost.

    Examples
    --------

    >>> constraint_rhs = pd.DataFrame({
    ...   'constraint_id': [1, 2, 3],
    ...   'type': ['>=', '<=', '='],
    ...   'cost': [14000.0, 14000.0, 14000.]})

    >>> deficit_variables, lhs = create_deficit_variables(constraint_rhs, 1)

    Note two variables are needed for equality constraints, one to allow violation up and one to allow violation down.

    >>> print(deficit_variables)
       variable_id     cost  lower_bound  upper_bound        type
    0            1  14000.0          0.0          inf  continuous
    1            2  14000.0          0.0          inf  continuous
    0            3  14000.0          0.0          inf  continuous
    0            4  14000.0          0.0          inf  continuous

    >>> print(lhs)
       variable_id  constraint_id  coefficient
    0            1              1          1.0
    1            2              2         -1.0
    0            3              3         -1.0
    0            4              3          1.0

    Parameters
    ----------
    constraint_rhs : pd.DataFrame
        ==============  ====================================================================
        Columns:        Description:
        constraint_id   the id of the constraint (as `int`)
        type            the type of the constraint, e.g. ">=" or "<=" (as `str`)
        cost            the cost of using the deficit variable to violate the constraint (as `np.float64`)
        ==============  ====================================================================

    Returns
    -------
    deficit_variables : pd.DataFrame
        =============  ====================================================================
        Columns:       Description:
        variable_id    the id of the variable (as `int`)
        lower_bound    the minimum value of the variable (as `np.float64`)
        upper_bound    the maximum value of the variable (as `np.float64`)
        type           the type of variable, is continuous for deficit variables  (as `str`)
        cost           the cost of using the deficit variable to violate the constraint (as `np.float64`)
        =============  ====================================================================

    lhs : pd.DataFrame
        =============  ====================================================================
        Columns:       Description:
        variable_id    the id of the variable (as `int`)
        constraint_id  the id of the constraint (as `int`)
        coefficient    the variable lhs coefficient (as `np.float64`)
        =============  ====================================================================
    """

    inequalities = constraint_rhs[constraint_rhs['type'].isin(['>=', '<='])]
    equalities = constraint_rhs[constraint_rhs['type'] == '=']

    inequalities = hf.save_index(inequalities.reset_index(drop=True), 'variable_id', next_variable_id)

    inequalities_deficit_variables = inequalities.loc[:, ['variable_id', 'cost']]
    inequalities_deficit_variables['lower_bound'] = 0.0
    inequalities_deficit_variables['upper_bound'] = np.inf
    inequalities_deficit_variables['type'] = 'continuous'

    inequalities_lhs = inequalities.loc[:, ['variable_id', 'constraint_id', 'type']]
    inequalities_lhs['coefficient'] = np.where(inequalities_lhs['type'] == '>=', 1.0, -1.0)
    inequalities_lhs = inequalities_lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

    if not equalities.empty:
        if not inequalities.empty:
            next_variable_id = inequalities['variable_id'].max() + 1
        equalities_up = hf.save_index(equalities.reset_index(drop=True), 'variable_id', next_variable_id)
        next_variable_id = equalities_up['variable_id'].max() + 1
        equalities_down = hf.save_index(equalities.reset_index(drop=True), 'variable_id', next_variable_id)

        equalities_up_deficit_variables = equalities_up.loc[:, ['variable_id', 'cost']]
        equalities_up_deficit_variables['lower_bound'] = 0.0
        equalities_up_deficit_variables['upper_bound'] = np.inf
        equalities_up_deficit_variables['type'] = 'continuous'

        equalities_down_deficit_variables = equalities_down.loc[:, ['variable_id', 'cost']]
        equalities_down_deficit_variables['lower_bound'] = 0.0
        equalities_down_deficit_variables['upper_bound'] = np.inf
        equalities_down_deficit_variables['type'] = 'continuous'

        equalities_up_lhs = equalities_up.loc[:, ['variable_id', 'constraint_id', 'type']]
        equalities_up_lhs['coefficient'] = -1.0
        equalities_up_lhs = equalities_up_lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

        equalities_down_lhs = equalities_down.loc[:, ['variable_id', 'constraint_id', 'type']]
        equalities_down_lhs['coefficient'] = 1.0
        equalities_down_lhs = equalities_down_lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

        deficit_variables = pd.concat([inequalities_deficit_variables, equalities_up_deficit_variables,
                                       equalities_down_deficit_variables])

        lhs = pd.concat([inequalities_lhs, equalities_up_lhs, equalities_down_lhs])

    else:
        deficit_variables = inequalities_deficit_variables
        lhs = inequalities_lhs

    return deficit_variables, lhs
