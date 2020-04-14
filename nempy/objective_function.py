import pandas as pd
from nempy import helper_functions as hf


def energy(variable_ids, price_bids, unit_info):
    """Create the cost coefficients of energy in bids in the objective function.

    This function defines the cost associated with each decision variable that represents a unit's energy bid. Costs are
    are with reference to the regional node. If a loss factor is provided in the unit info, this calculated by dividing
    the bid cost by the assumed loss factor, if no loss factor is provided then the costs are used as is.

    :param variable_ids: DataFrame
        unit: str
        1: float
        2: float
        .
        .
        .
        n: float
    :param price_bids: DataFrame
        unit: str
        capacity_band: str
        variable_id: int
    :param unit_info: DataFrame
        unit: str
        loss_factor
    :return: DataFrame
        variable_id: int
        cost: float
    """
    # Get the list of columns that are bid bands.
    bid_bands = [col for col in price_bids.columns if col != 'unit']
    # Reshape the DataFrame such that all bids are stacked vertical e.g
    # from:
    # unit 1  2
    # A    10 20
    # B    10 10
    #
    # to:
    # unit capacity_band cost
    # A    1             10
    # A    2             20
    # B    1             10
    # B    2             10
    price_bids = hf.stack_columns(price_bids, cols_to_keep=['unit'], cols_to_stack=bid_bands,
                                  type_name='capacity_band', value_name='cost')
    # Match bid cost with existing variable ids
    objective_function = pd.merge(variable_ids, price_bids, how='inner', on=['unit', 'capacity_band'])
    # Match units with their loss factors.
    objective_function = pd.merge(objective_function, unit_info, how='inner', on='unit')
    # Refer bids cost to regional reference node, if a loss factor  was provided.
    if 'loss_factor' in objective_function.columns:
        objective_function['cost'] = objective_function['cost'] / objective_function['loss_factor']
    # Return only variable costs.
    objective_function = objective_function.loc[:, ['variable_id', 'cost']]
    return objective_function
