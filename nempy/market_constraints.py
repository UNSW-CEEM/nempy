from nempy import helper_functions as hf
import pandas as pd


def energy(demand, next_constraint_id):
    """Create the constraints that ensure the amount of supply dispatched in each region equals demand.

    If only one region exists then the constraint will be of the form:

        unit 1 output + unit 2 output +. . .+ unit n output = region demand

    If multiple regions exist then a constraint will ne created for each region. If there were 2 units A and B in region
    X, and 2 units C and D in region Y, then the constraints would be of the form:

        constraint 1: unit A output + unit B output = region X demand
        constraint 2: unit C output + unit D output = region Y demand

    The constraints are returned in two DataFrames, one for the lhs coefficients, and one for constraint rhs, type and
    other information. Assuming regions X's demand was 100 MW and region Y's demand was 200 MW, the next free
    constraint id was 1, and unit A, B, C, D had constraint ids of 1, 2, 3, 4. Then for the above two region example
    these DataFrames would look like

    constraints_lhs:

        constraint_id variable_id coefficient
        1             1           1
        1             2           1
        2             3           1
        2             4           1

    and constraints_rhs:

        region constraint_id type rhs
        X      1             =    100
        Y      2             =    200

    :param energy_bid_ids: DataFrame
        variable_id: int
        unit: str
    :param demand: DataFrame
        region: str
        demand: float
    :param unit_info:
        unit: str
        region: str
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
    # Create an index for each constraint.
    constraints_rhs = hf.save_index(demand, 'constraint_id', next_constraint_id)
    # Set constraint level values.
    constraints_rhs['type'] = '='
    constraints_rhs['rhs'] = constraints_rhs['demand']
    constraints_rhs['service'] = 'energy'
    constraints_rhs['coefficient'] = 1.0
    # Return just the needed columns.
    constraints_rhs = constraints_rhs.loc[:, ['region', 'constraint_id', 'type', 'rhs', 'coefficient', 'service']]
    return constraints_rhs
