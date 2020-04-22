import pandas as pd
from nempy import helper_functions as hf


def capacity(energy_bid_ids, unit_limits, next_constraint_id):
    """Create the constraints that ensure the dispatch of a unit is capped by its capacity.

    A constraint of the following form will be created for each unit:

        bid 1 dispatched + bid 2 dispatched +. . .+ bid n dispatched <= capacity

    The constraints are returned in two DataFrames, one for the lhs coefficients, and one for constraint rhs, type.
    Assuming there were to units A and B, with three bids each, with ids 1, 2, 3, and 4, 5, 6, respectively, and
    capacities 50 MW and 60 MW, and a next_constraint_id value of 0. Then the resulting DataFrames would be:

    constraints_lhs:

        constraint_id variable_id coefficient
        0             1           1
        0             2           1
        0             3           1
        1             4           1
        1             5           1
        1             6           1

    and constraints_rhs:

        constraint_id type rhs
        0             <=   50
        1             <=   60

    :param energy_bid_ids: DataFrame
        variable_id: int
        unit: str
    :param unit_limits: DataFrame
        unit: str
        capacity: float
    :param next_constraint_id: int
    :return:
        constraints_lhs: DataFrame
            constraint_id: int
            variable_id: int
            coefficient: float
        constraints_rhs: DataFrame
            region: str
            constraint_id: int
            type: str
            rhs: float
    """
    capacity_constraints = create_constraints(energy_bid_ids, unit_limits, next_constraint_id, 'capacity', '<=')
    constraints_lhs = capacity_constraints.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    constraints_rhs = capacity_constraints.loc[:, ['unit', 'constraint_id', 'type', 'rhs']].\
        drop_duplicates('constraint_id')
    return constraints_lhs, constraints_rhs


def ramp_up(bidding_ids, unit_limits, next_constraint_id, dispatch_interval):
    """Create the constraints that ensure the dispatch of a unit is capped by its ramp up rate.

    A constraint of the following form will be created for each unit:

        bid 1 dispatched + bid 2 dispatched +. . .+ bid n dispatched <= initial output + ramp up rate

    The constraints are returned in two DataFrames, one for the lhs coefficients, and one for constraint rhs and type.
    Assuming there were to units A and B, with three bids each, with ids 1, 2, 3, and 4, 5, 6, respectively, and
    initial outputs of 30 MW and 40 MW, ramp rates of 600 and 1200 MW/h, a next_constraint_id value of 0 and a
    dispatch interval of 5 min. Then the resulting DataFrames would be:

    constraints_lhs:

        constraint_id variable_id coefficient
        0             1           1
        0             2           1
        0             3           1
        1             4           1
        1             5           1
        1             6           1

    and constraints_rhs:

        constraint_id type rhs
        0             <=   100
        1             <=   160

    :param energy_bid_ids: DataFrame
        variable_id: int
        unit: str
    :param unit_limits: DataFrame
        unit: str
        capacity: float
    :param next_constraint_id: int
    :return:
        constraints_lhs: DataFrame
            constraint_id: int
            variable_id: int
            coefficient: float
        constraints_rhs: DataFrame
            constraint_id: int
            type: str
            rhs: float
    """
    unit_limits['max_output'] = unit_limits['initial_output'] + unit_limits['ramp_up_rate'] * (dispatch_interval / 60)
    capacity_constraints = create_constraints(bidding_ids, unit_limits, next_constraint_id, 'max_output', '<=')
    constraints_lhs = capacity_constraints.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    constraints_rhs = capacity_constraints.loc[:, ['constraint_id', 'type', 'rhs']].drop_duplicates('constraint_id')
    return constraints_lhs, constraints_rhs


def ramp_down(bidding_ids, unit_limits, next_constraint_id, dispatch_interval):
    """Create the constraints that ensure the dispatch of a unit is capped by its ramp down rate.

    A constraint of the following form will be created for each unit:

        bid 1 dispatched + bid 2 dispatched +. . .+ bid n dispatched <= initial output - ramp down rate

    The constraints are returned in two DataFrames, one for the lhs coefficients, and one for constraint rhs and type.
    Assuming there were to units A and B, with three bids each, with ids 1, 2, 3, and 4, 5, 6, respectively, and
    initial outputs of 200 MW and 300 MW, ramp rates of 600 and 1200 MW/h, a next_constraint_id value of 0 and a
    dispatch interval of 5 min. Then the resulting DataFrames would be:

    constraints_lhs:

        constraint_id variable_id coefficient
        0             1           1
        0             2           1
        0             3           1
        1             4           1
        1             5           1
        1             6           1

    and constraints_rhs:

        constraint_id type rhs
        0             <=   100
        1             <=   200

    :param bidding_ids: DataFrame
        variable_id: int
        unit: str
    :param unit_limits: DataFrame
        unit: str
        capacity: float
    :param next_constraint_id: int
    :param dispatch_interval: int
    :return:
        constraints_lhs: DataFrame
            constraint_id: int
            variable_id: int
            coefficient: float
        constraints_rhs: DataFrame
            constraint_id: int
            type: str
            rhs: float
    """
    unit_limits['min_output'] = unit_limits['initial_output'] - unit_limits['ramp_down_rate'] * (dispatch_interval / 60)
    capacity_constraints = create_constraints(bidding_ids, unit_limits, next_constraint_id, 'min_output', '>=')
    constraints_lhs = capacity_constraints.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    constraints_rhs = capacity_constraints.loc[:, ['constraint_id', 'type', 'rhs']].drop_duplicates('constraint_id')
    return constraints_lhs, constraints_rhs


def create_constraints(bidding_ids, unit_limits, next_constraint_id, rhs_col, direction):
    # Create constraint row indexes for each unit.
    constraint_rows = hf.save_index(unit_limits.reset_index(drop=True), 'constraint_id', next_constraint_id)
    constraint_rows = constraint_rows.loc[:, ['unit', 'constraint_id', rhs_col]]
    # Merge in bidding data to constraint row data.
    constraints = pd.merge(constraint_rows, bidding_ids, how='inner', on='unit')
    constraints['rhs'] = constraints[rhs_col]
    constraints = constraints.loc[:, ['variable_id', 'constraint_id', 'unit', 'rhs']]
    constraints['coefficient'] = 1
    constraints['type'] = direction
    return constraints