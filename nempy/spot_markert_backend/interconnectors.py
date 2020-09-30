import numpy as np
import pandas as pd
from nempy.help_functions import helper_functions as hf


def create(definitions, next_variable_id):
    """Create decision variables, and their mapping to constraints. For modeling interconnector flows. As DataFrames.

    Examples
    --------
    Definitions for two interconnectors, one called A, that nominal flows from region X to region Y, note A can flow in
    both directions because of the way max and min are defined. The interconnector B nominal flows from Y to Z, but can
    only flow in the forward direction.

    >>> pd.options.display.width = None

    >>> inter_definitions = pd.DataFrame({
    ...   'interconnector': ['A', 'B'],
    ...   'link': ['A', 'B'],
    ...   'from_region': ['X', 'Y'],
    ...   'to_region': ['Y', 'Z'],
    ...   'max': [100.0, 400.0],
    ...   'min': [-100.0, 50.0],
    ...   'generic_constraint_factor': [1, 1],
    ...   'from_region_loss_factor': [0.9, 1.0],
    ...   'to_region_loss_factor': [1.0, 1.1]})

    >>> print(inter_definitions)
      interconnector link from_region to_region    max    min  generic_constraint_factor  from_region_loss_factor  to_region_loss_factor
    0              A    A           X         Y  100.0 -100.0                          1                      0.9                    1.0
    1              B    B           Y         Z  400.0   50.0                          1                      1.0                    1.1

    Start creating new variable ids from 0.

    >>> next_variable_id = 0

    Run the function and print results.

    >>> decision_variables, constraint_map = create(inter_definitions, next_variable_id)

    >>> print(decision_variables)
      interconnector link  variable_id  lower_bound  upper_bound        type  generic_constraint_factor
    0              A    A            0       -100.0        100.0  continuous                          1
    1              B    B            1         50.0        400.0  continuous                          1

    >>> print(constraint_map)
       variable_id interconnector link region service  coefficient
    0            0              A    A      Y  energy          1.0
    1            1              B    B      Z  energy          1.1
    2            0              A    A      X  energy         -0.9
    3            1              B    B      Y  energy         -1.0

    """

    # Create a variable_id for each interconnector.
    decision_variables = hf.save_index(definitions, 'variable_id', next_variable_id)

    # Create two entries in the constraint_map for each interconnector. This means the variable will be mapped to the
    # demand constraint of both connected regions.
    constraint_map = hf.stack_columns(decision_variables, ['variable_id', 'interconnector', 'link', 'max', 'min'],
                                      ['to_region', 'from_region'], 'direction', 'region')
    loss_factors = hf.stack_columns(decision_variables, ['variable_id'],
                                    ['from_region_loss_factor', 'to_region_loss_factor'], 'direction', 'loss_factor')
    loss_factors['direction'] = loss_factors['direction'].apply(lambda x: x.replace('_loss_factor', ''))
    constraint_map = pd.merge(constraint_map, loss_factors, on=['variable_id', 'direction'])

    # Define decision variable attributes.
    decision_variables['type'] = 'continuous'
    decision_variables = decision_variables.loc[:, ['interconnector', 'link', 'variable_id', 'min', 'max', 'type',
                                                    'generic_constraint_factor']]
    decision_variables.columns = ['interconnector', 'link',  'variable_id', 'lower_bound', 'upper_bound', 'type',
                                  'generic_constraint_factor']

    # Set positive coefficient for the to_region so the interconnector flowing in the nominal direction helps meet the
    # to_region demand constraint. Negative for the from_region, same logic.
    constraint_map['coefficient'] = np.where(constraint_map['direction'] == 'to_region',
                                             1.0 * constraint_map['loss_factor'],
                                             -1.0 * constraint_map['loss_factor'])
    constraint_map['service'] = 'energy'
    constraint_map = constraint_map.loc[:, ['variable_id', 'interconnector', 'link',
                                            'region', 'service', 'coefficient']]

    return decision_variables, constraint_map


def link_inter_loss_to_interpolation_weights(weight_variables, loss_variables, loss_functions, next_constraint_id):
    """
    Examples
    --------

    Setup function inputs

    >>> loss_variables = pd.DataFrame({
    ...   'interconnector': ['I'],
    ...   'link': ['I'],
    ...   'variable_id': [0]})

    >>> weight_variables = pd.DataFrame({
    ...   'interconnector': ['I', 'I', 'I'],
    ...   'link': ['I', 'I', 'I'],
    ...   'variable_id': [1, 2, 3],
    ...   'break_point': [-100.0, 0, 100.0]})

    Loss functions can arbitrary, they just need to take the flow as input and return losses as an output.

    >>> def constant_losses(flow):
    ...     return abs(flow) * 0.05

    The loss function get assigned to an interconnector by its row in the loss functions DataFrame.

    >>> loss_functions = pd.DataFrame({
    ...    'interconnector': ['I'],
    ...    'link': ['I'],
    ...    'from_region_loss_share': [0.5],
    ...    'loss_function': [constant_losses]})

    >>> next_constraint_id = 0

    Create the constraints.

    >>> lhs, rhs = link_inter_loss_to_interpolation_weights(weight_variables, loss_variables, loss_functions,
    ...                                                     next_constraint_id)

    >>> print(lhs)
       variable_id  constraint_id  coefficient
    0            1              0          5.0
    1            2              0          0.0
    2            3              0          5.0

    >>> print(rhs)
      interconnector link  constraint_id type  rhs_variable_id
    0              I    I              0    =                0
    """

    # Create a constraint for each set of weight variables.
    constraint_ids = weight_variables.loc[:, ['interconnector', 'link']].drop_duplicates(['interconnector', 'link'])
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)

    # Map weight variables to their corresponding constraints.
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'link', 'variable_id', 'break_point']],
                   constraint_ids, 'inner', on=['interconnector', 'link'])
    lhs = pd.merge(lhs, loss_functions.loc[:, ['interconnector', 'link', 'loss_function']], 'inner',
                   on=['interconnector', 'link'])

    # Evaluate the loss function at each break point to get the lhs coefficient.
    lhs['coefficient'] = lhs.apply(lambda x: x['loss_function'](x['break_point']), axis=1)
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

    # Get the loss variables that will be on the rhs of the constraints.
    rhs_variables = loss_variables.loc[:, ['interconnector', 'link', 'variable_id']]
    rhs_variables.columns = ['interconnector', 'link', 'rhs_variable_id']
    # Map the rhs variables to their constraints.
    rhs = pd.merge(constraint_ids, rhs_variables, 'inner', on=['interconnector', 'link'])
    rhs['type'] = '='
    rhs = rhs.loc[:, ['interconnector', 'link', 'constraint_id', 'type', 'rhs_variable_id']]
    return lhs, rhs


def link_weights_to_inter_flow(weight_variables, flow_variables, next_constraint_id):
    """
    Examples
    --------

    Setup function inputs

    >>> flow_variables = pd.DataFrame({
    ...   'interconnector': ['I'],
    ...   'link': ['I'],
    ...   'variable_id': [0]})

    >>> weight_variables = pd.DataFrame({
    ...   'interconnector': ['I', 'I', 'I'],
    ...   'link': ['I', 'I', 'I'],
    ...   'variable_id': [1, 2, 3],
    ...   'break_point': [-100.0, 0, 100.0]})

    >>> next_constraint_id = 0

    Create the constraints.

    >>> lhs, rhs = link_weights_to_inter_flow(weight_variables, flow_variables, next_constraint_id)

    >>> print(lhs)
       variable_id  constraint_id  coefficient
    0            1              0       -100.0
    1            2              0          0.0
    2            3              0        100.0

    >>> print(rhs)
      interconnector link  constraint_id type  rhs_variable_id
    0              I    I              0    =                0
    """

    # Create a constraint for each set of weight variables.
    constraint_ids = weight_variables.loc[:, ['interconnector', 'link']].drop_duplicates(['interconnector', 'link'])
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)

    # Map weight variables to their corresponding constraints.
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'link', 'variable_id', 'break_point']],
                   constraint_ids, 'inner', on=['interconnector', 'link'])
    lhs['coefficient'] = lhs['break_point']
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

    # Get the interconnector variables that will be on the rhs of constraint.
    rhs_variables = flow_variables.loc[:, ['interconnector', 'link', 'variable_id']]
    rhs_variables.columns = ['interconnector', 'link', 'rhs_variable_id']
    # Map the rhs variables to their constraints.
    rhs = pd.merge(constraint_ids, rhs_variables, 'inner', on=['interconnector', 'link'])
    rhs['type'] = '='
    rhs = rhs.loc[:, ['interconnector', 'link', 'constraint_id', 'type', 'rhs_variable_id']]
    return lhs, rhs


def create_weights_must_sum_to_one(weight_variables, next_constraint_id):
    """Create the constraint to force weight variable to sum to one, need for interpolation to work.

    For one interconnector, if we had  three weight variables w1, w2, and w3, then the constraint would be of the form.

        w1 * 1.0 + w2 * 1.0 + w3 * 1.0 = 1.0

    Examples
    --------

    Setup function inputs

    >>> weight_variables = pd.DataFrame({
    ...   'interconnector': ['I', 'I', 'I'],
    ...   'link': ['I', 'I', 'I'],
    ...   'variable_id': [1, 2, 3],
    ...   'break_point': [-100.0, 0, 100.0]})

    >>> next_constraint_id = 0

    Create the constraints.

    >>> lhs, rhs = create_weights_must_sum_to_one(weight_variables, next_constraint_id)

    >>> print(lhs)
       variable_id  constraint_id  coefficient
    0            1              0          1.0
    1            2              0          1.0
    2            3              0          1.0

    >>> print(rhs)
      interconnector link  constraint_id type  rhs
    0              I    I              0    =  1.0

    """

    # Create a constraint for each set of weight variables.
    constraint_ids = weight_variables.loc[:, ['interconnector', 'link']].drop_duplicates(['interconnector', 'link'])
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)

    # Map weight variables to their corresponding constraints.
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'link', 'variable_id']], constraint_ids,
                   'inner', on=['interconnector', 'link'])
    lhs['coefficient'] = 1.0
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

    # Create rhs details for each constraint.
    rhs = constraint_ids
    rhs['type'] = '='
    rhs['rhs'] = 1.0
    return lhs, rhs


def create_weights(break_points, next_variable_id):
    """Create interpolation weight variables for each breakpoint.

    Examples
    --------

    >>> break_points = pd.DataFrame({
    ...   'interconnector': ['I', 'I', 'I'],
    ...   'loss_segment': [1, 2, 3],
    ...   'break_point': [-100.0, 0.0, 100.0]})

    >>> next_variable_id = 0

    >>> weight_variables = create_weights(break_points, next_variable_id)

    >>> print(weight_variables.loc[:, ['interconnector', 'loss_segment', 'break_point', 'variable_id']])
      interconnector  loss_segment  break_point  variable_id
    0              I             1       -100.0            0
    1              I             2          0.0            1
    2              I             3        100.0            2

    >>> print(weight_variables.loc[:, ['variable_id', 'lower_bound', 'upper_bound', 'type']])
       variable_id  lower_bound  upper_bound        type
    0            0          0.0          1.0  continuous
    1            1          0.0          1.0  continuous
    2            2          0.0          1.0  continuous

    Parameters
    ----------
    break_points : pd.DataFrame
        ==============  ================================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        loss_segment    unique identifier of a loss segment on an interconnector basis (as `np.float64`)
        break_points    the interconnector flow values to interpolate losses between (as `np.float64`)
        ==============  ================================================================================

    next_variable_id : int

    Returns
    -------
    weight_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        loss_segment    unique identifier of a loss segment on an interconnector basis (as `np.float64`)
        break_points    the interconnector flow values to interpolate losses between (as `np.int64`)
        variable_id     the id of the variable (as `np.int64`)
        lower_bound    the lower bound of the variable, is zero for weight variables (as `np.float64`)
        upper_bound    the upper bound of the variable, is one for weight variables (as `np.float64`)
        type           the type of variable, is continuous for bids  (as `str`)
        ==============  ==============================================================================
    """
    # Create a variable for each break point.
    weight_variables = hf.save_index(break_points, 'variable_id', next_variable_id)
    weight_variables['lower_bound'] = 0.0
    weight_variables['upper_bound'] = 1.0
    weight_variables['type'] = 'continuous'
    return weight_variables


def create_loss_variables(inter_variables, inter_constraint_map, loss_shares, next_variable_id):
    """
    Examples
    --------
    Setup function inputs

    >>> inter_variables = pd.DataFrame({
    ...   'interconnector': ['I'],
    ...   'link': ['i'],
    ...   'lower_bound': [-50.0],
    ...   'upper_bound': [100.0],
    ...   'type': ['continuous']})

    >>> inter_constraint_map = pd.DataFrame({
    ... 'interconnector': ['I', 'I'],
    ... 'link': ['i', 'i'],
    ... 'region': ['X', 'Y'],
    ... 'service': ['energy', 'energy'],
    ... 'coefficient': [1.0, -1.0]})

    >>> loss_shares = pd.DataFrame({
    ...    'interconnector': ['I'],
    ...    'link': ['i'],
    ...    'from_region_loss_share': [0.5],
    ...    'from_region': ['X']})

    >>> next_constraint_id = 0

    Create the constraints.

    >>> loss_variables, constraint_map = create_loss_variables(inter_variables, inter_constraint_map, loss_shares,
    ...                                                        next_constraint_id)

    >>> print(loss_variables)
      interconnector link  variable_id  lower_bound  upper_bound        type
    0              I    i            0       -100.0        100.0  continuous

    >>> print(constraint_map)
       variable_id region service  coefficient
    0            0      X  energy         -0.5
    1            0      Y  energy         -0.5
    """

    # Preserve the interconnector variable id for merging later.
    columns_for_loss_variables = inter_variables.loc[:, ['interconnector', 'link', 'lower_bound',
                                                         'upper_bound', 'type']]
    columns_for_loss_variables['upper_bound'] = \
        columns_for_loss_variables.loc[:, ['lower_bound', 'upper_bound']].abs().max(axis=1)
    columns_for_loss_variables['lower_bound'] = -1 * columns_for_loss_variables['upper_bound']

    inter_constraint_map = inter_constraint_map.loc[:, ['interconnector', 'link', 'region', 'service', 'coefficient']]

    # Create a variable id for loss variables
    loss_variables = hf.save_index(columns_for_loss_variables, 'variable_id', next_variable_id)
    loss_variables = pd.merge(loss_variables,
                              loss_shares.loc[:, ['interconnector', 'link', 'from_region_loss_share', 'from_region']],
                              on=['interconnector', 'link'])

    # Create the loss variable constraint map by combining the new variables and the flow variable constraint map.
    constraint_map = pd.merge(
        loss_variables.loc[:, ['variable_id', 'interconnector', 'link', 'from_region_loss_share', 'from_region']],
        inter_constraint_map, 'inner', on=['interconnector', 'link'])

    # Assign losses to regions according to the from_region_loss_share
    constraint_map['coefficient'] = np.where(constraint_map['from_region'] == constraint_map['region'],
                                             - 1 * constraint_map['from_region_loss_share'],
                                             - 1 * (1 - constraint_map['from_region_loss_share']))

    loss_variables = loss_variables.loc[:, ['interconnector', 'link', 'variable_id',
                                            'lower_bound', 'upper_bound', 'type']]
    constraint_map = constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]
    return loss_variables, constraint_map
