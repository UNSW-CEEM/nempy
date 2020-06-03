import pandas as pd
import numpy as np
from nempy import helper_functions as hf, check


def create_deficit_variables(constraint_rhs, next_variable_id):
    """ Create variables that allow a constraint to violated at a specified cost.

    Examples
    --------

    >>> constraint_rhs = pd.DataFrame({
    ...   'constraint_id': [1, 2],
    ...   'type': ['>=', '<='],
    ...   'violation_cost': [14000.0, 14000.0]})

    >>> deficit_variables, lhs = create_deficit_variables(constraint_rhs, 1)

    >>> print(deficit_variables)
       variable_id     cost  lower_bound  upper_bound        type
    0            1  14000.0          0.0          inf  continuous
    1            2  14000.0          0.0          inf  continuous

    >>> print(lhs)
       variable_id  constraint_id  coefficient
    0            1              1          1.0
    1            2              2         -1.0

    Parameters
    ----------
    constraint_rhs : pd.DataFrame
        ==============  ====================================================================
        Columns:        Description:
        constraint_id   the id of the constraint (as `int`)
        type            the type of the constraint, e.g. ">=" or "<=" (as `str`)
        violation_cost  the cost of using the deficit variable to violate the constraint (as `np.float64`)
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
    if '=' in list(constraint_rhs['type']):
        raise check.ColumnValues("Elastic constraints only supported for types >= and <= not type =.")

    constraint_rhs = hf.save_index(constraint_rhs.reset_index(drop=True), 'variable_id', next_variable_id)

    deficit_variables = constraint_rhs.loc[:, ['variable_id', 'violation_cost']]
    deficit_variables.columns = ['variable_id', 'cost']
    deficit_variables['lower_bound'] = 0.0
    deficit_variables['upper_bound'] = np.inf
    deficit_variables['type'] = 'continuous'

    lhs = constraint_rhs.loc[:, ['variable_id', 'constraint_id', 'type']]
    lhs['coefficient'] = np.where(lhs['type'] == '>=', 1.0, -1.0)
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

    return deficit_variables, lhs
