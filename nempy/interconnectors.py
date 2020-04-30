import numpy as np
import pandas as pd
from nempy import helper_functions as hf


def create(definitions, next_variable_id):
    """Create decision variables, and their mapping to constraints. For modeling interconnector flows. As DataFrames.

    Examples
    --------
    >>> import pandas

    Definitions for two interconnectors, one called A, that nominal flows from region X to region Y, note A can flow in
    both directions because of the way max and min are defined. The interconnector B nominal flows from Y to Z, but can
    only flow in the forward direction.

    >>> inter_definitions = pd.DataFrame({
    ...   'interconnector': ['A', 'B'],
    ...   'from_region': ['X', 'Y'],
    ...   'to_region': ['Y', 'Z'],
    ...   'max': [100.0, 400.0],
    ...   'min': [-100.0, 50.0]})

    >>> print(inter_definitions)
      interconnector from_region to_region    max    min
    0              A           X         Y  100.0 -100.0
    1              B           Y         Z  400.0   50.0

    Start creating new variable ids from 0.

    >>> next_variable_id = 0

    Run the function and print results.

    >>> decision_variables, constraint_map = create(inter_definitions, next_variable_id)

    >>> print(decision_variables)
      interconnector  variable_id  lower_bound  upper_bound        type
    0              A            0       -100.0        100.0  continuous
    1              B            1         50.0        400.0  continuous

    >>> print(constraint_map)
       variable_id region service  coefficient
    0            0      Y  energy          1.0
    1            1      Z  energy          1.0
    2            0      X  energy         -1.0
    3            1      Y  energy         -1.0

    Parameters
    ----------
    definitions : pd.DataFrame
            Interconnector definition.

            ==============  =====================================================================================
            Columns:        Description:
            interconnector  unique identifier of a interconnector (as `str`)
            to_region       the region that receives power when flow is in the positive direction (as `str`)
            from_region     the region that power is drawn from when flow is in the positive direction (as `str`)
            max             the maximum power flow in the positive direction, in MW (as `np.float64`)
            min             the maximum power flow in the negative direction, in MW (as `np.float64`)
            ==============  =====================================================================================

    next_variable_id : int

    Returns
    -------
    decision_variables : pd.DataFrame

        =============  ===============================================================
        Columns:       Description:
        variable_id    the id of the variable (as `int`)
        lower_bound    the lower bound of the variable, the min interconnector flow (as `float`)
        upper_bound    the upper bound of the variable, the max inerconnector flow (as `float`)
        type           the type of variable, is continuous for interconnectors  (as `str`)
        =============  ===============================================================

    constraint_map : pd.DataFrame
        Sets out which regional demand constraints the variable should be linked to.

        =============  ===================================================================================
        Columns:       Description:
        variable_id    the id of the variable (as `int`)
        region         the regional constraints to map the variable to  (as `str`)
        service        the service type constraints to map too, only energy for interconnectors (as `str`)
        coefficient    the variable side contribution to the coefficient (as `np.float64`)
        =============  ====================================================================================
    """

    # Create a variable_id for each interconnector.
    decision_variables = hf.save_index(definitions, 'variable_id', next_variable_id)

    # Create two entries in the constraint_map for each interconnector. This means the variable will be mapped to the
    # demand constraint of both connected regions.
    constraint_map = hf.stack_columns(decision_variables, ['variable_id', 'interconnector', 'max', 'min'],
                                      ['to_region', 'from_region'], 'direction', 'region')

    # Define decision variable attributes.
    decision_variables['type'] = 'continuous'
    decision_variables = decision_variables.loc[:, ['interconnector', 'variable_id', 'min', 'max', 'type']]
    decision_variables.columns = ['interconnector',  'variable_id', 'lower_bound', 'upper_bound', 'type']

    # Set positive coefficient for the to_region so the interconnector flowing in the nominal direction helps meet the
    # to_region demand constraint. Negative for the from_region, same logic.
    constraint_map['coefficient'] = np.where(constraint_map['direction'] == 'to_region', 1.0, -1.0)
    constraint_map['service'] = 'energy'
    constraint_map = constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]

    return decision_variables, constraint_map


def link_weights_to_inter_loss(weight_variables, loss_variables, loss_functions, next_constraint_id):
    constraint_ids = weight_variables.loc[:, ['interconnector']].drop_duplicates('interconnector')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'variable_id', 'break_point']], constraint_ids, 'inner',
                   on='interconnector')
    lhs = pd.merge(lhs, loss_functions, 'inner', on='interconnector')
    lhs['coefficient'] = lhs.apply(lambda x: x['loss_function'](x['break_point']), axis=1)
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    rhs = pd.merge(constraint_ids,
                   loss_variables.drop_duplicates('variable_id').loc[:, ['variable_id', 'interconnector']],
                   'inner', on='interconnector')
    rhs['rhs_variable_id'] = rhs['variable_id']
    rhs['type'] = '='
    rhs = rhs.loc[:, ['interconnector', 'constraint_id', 'rhs_variable_id', 'type']]
    return lhs, rhs


def link_weights_to_inter_flow(weight_variables, flow_variables, next_constraint_id):
    constraint_ids = weight_variables.loc[:, ['interconnector']].drop_duplicates('interconnector')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'variable_id', 'break_point']], constraint_ids, 'inner',
                   on='interconnector')
    lhs['coefficient'] = lhs['break_point']
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    rhs = pd.merge(constraint_ids,
                   flow_variables.drop_duplicates('variable_id').loc[:, ['variable_id', 'interconnector']],
                   'inner', on='interconnector')
    rhs['rhs_variable_id'] = rhs['variable_id']
    rhs['type'] = '='
    rhs = rhs.loc[:, ['interconnector', 'constraint_id', 'rhs_variable_id', 'type']]
    return lhs, rhs


def create_weights_must_sum_to_one(weight_variables, next_constraint_id):
    constraint_ids = weight_variables.loc[:, ['interconnector']].drop_duplicates('interconnector')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'variable_id']], constraint_ids, 'inner',
                   on='interconnector')
    lhs['coefficient'] = 1.0
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    rhs = constraint_ids
    rhs['rhs'] = 1
    rhs['type'] = '='
    return lhs, rhs


def create_weights(break_points, next_variable_id):
    weight_variables = hf.save_index(break_points, 'variable_id', next_variable_id)
    weight_variables['lower_bound'] = 0.0
    weight_variables['upper_bound'] = 1.0
    weight_variables['type'] = 'continuous'
    return weight_variables


def create_loss_variables(inter_variables, inter_constraint_map, loss_function, next_variable_id):
    # Preserve the interconnector variable id for merging later.
    columns_for_loss_variables = \
        inter_variables.loc[:, ['interconnector', 'variable_id', 'lower_bound', 'upper_bound', 'type']]
    columns_for_loss_variables.columns = ['interconnector', 'inter_variable_id', 'lower_bound', 'upper_bound', 'type']
    inter_constraint_map = inter_constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]
    inter_constraint_map.columns = ['inter_variable_id', 'region', 'service', 'coefficient']

    # Create a variable id for loss variables
    loss_variables = hf.save_index(loss_function.loc[:, ['interconnector', 'from_region_loss_share']], 'variable_id',
                                   next_variable_id)
    # Use interconnector variable definitions to formulate loss variable definitions.
    columns_for_loss_variables['upper_bound'] = \
        columns_for_loss_variables.loc[:, ['lower_bound', 'upper_bound']].abs().max(axis=1)
    columns_for_loss_variables['lower_bound'] = 0.0
    loss_variables = pd.merge(loss_variables, columns_for_loss_variables, 'inner', on='interconnector')

    constraint_map = pd.merge(
        loss_variables.loc[:, ['variable_id', 'inter_variable_id', 'interconnector', 'from_region_loss_share']],
        inter_constraint_map, 'inner', on='inter_variable_id')

    constraint_map['coefficient'] = np.where(constraint_map['coefficient'] < 0.0,
                                             - 1 * constraint_map['from_region_loss_share'],
                                             - 1 * (1 - constraint_map['from_region_loss_share']))

    loss_variables = loss_variables.loc[:, ['interconnector', 'variable_id', 'lower_bound', 'upper_bound', 'type']]
    constraint_map = constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]
    return loss_variables, constraint_map
