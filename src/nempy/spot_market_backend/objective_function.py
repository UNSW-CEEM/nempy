import pandas as pd
import numpy as np
from nempy.help_functions import helper_functions as hf


def bids(variable_ids, price_bids):
    """Create the cost coefficients of energy in bids in the objective function.

    This function defines the cost associated with each decision variable that represents a unit's energy bid. Costs are
    with reference to the regional node.
    """
    # If no service column is provided assume bids are for energy.
    if 'service' not in price_bids.columns:
        price_bids['service'] = 'energy'

    if 'dispatch_type' not in price_bids.columns:
        price_bids['dispatch_type'] = 'generator'

    # Get the list of columns that are bid bands.
    bid_bands = [col for col in price_bids.columns if col not in ['unit', 'service']]
    price_bids = hf.stack_columns(price_bids, cols_to_keep=['unit', 'service', 'dispatch_type'], cols_to_stack=bid_bands,
                                  type_name='capacity_band', value_name='cost')
    # Match bid cost with existing variable ids
    objective_function = pd.merge(variable_ids, price_bids, how='inner',
                                  on=['unit', 'service', 'dispatch_type', 'capacity_band'])
    objective_function['cost'] = np.where((objective_function['dispatch_type'] == 'load') &
                                          (objective_function['service'] == 'energy'),
                                          -1.0 * objective_function['cost'], objective_function['cost'])
    return objective_function


def scale_by_loss_factors(objective_function, unit_info):
    """
    Scale the bid cost by dividing by the loss factor.

    Parameters
    ----------
    objective_function : pd.DataFrame
        Cost by variable id, also including unit and capacity band so loss factors can be applied if provided.

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        dispatch_type  "load" or "generator", optional default
                       'generator' (as `str`)
        capacity_band  the bid band of the variable (as `str`)
        variable_id    the id of the variable (as `int`)
        =============  ===============================================================

    unit_info : pd.DataFrame
        The loss factor to scale bids by.

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        dispatch_type  "load" or "generator", optional default
                       'generator' (as `str`)
        loss_factor    the id of the variable (as `int`)
        =============  ===============================================================

    Returns
    -------
    pd.DataFrame

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        dispatch_type  "load" or "generator", optional default
                       'generator' (as `str`)
        capacity_band  the bid band of the variable (as `str`)
        variable_id    the id of the variable (as `int`)
        cost           the bid cost of the variable (as `float`)
        =============  ===============================================================
    """

    # Match units with their loss factors.
    objective_function = pd.merge(objective_function, unit_info, how='inner', on=['unit', 'dispatch_type'])
    # Refer bids cost to regional reference node, if a loss factor  was provided.
    objective_function['cost'] = np.where(objective_function['service'] == 'energy',
                                          objective_function['cost'] / objective_function['loss_factor'],
                                          objective_function['cost'])
    return objective_function
