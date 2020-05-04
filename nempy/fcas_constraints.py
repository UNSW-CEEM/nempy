import pandas as pd
import numpy as np
from nempy import helper_functions as hf


def joint_ramping_constraints(fcas_regulation_trapeziums, unit_limits, dispatch_interval, next_constraint_id):
    constraints = hf.save_index(fcas_regulation_trapeziums, 'constraint_id', next_constraint_id)
    constraints = pd.merge(constraints, unit_limits, 'left', on='unit')
    constraints['rhs'] = np.where(
        constraints['service'] == 'raise_reg',
        constraints['initial_output'] + constraints['ramp_up_rate'] / (dispatch_interval / 60),
        constraints['initial_output'] - constraints['ramp_up_rate'] / (dispatch_interval / 60))
    constraints['type'] = np.where(constraints['service'] == 'raise_reg', '<=', '>=')

    variable_mapping_reg = constraints.loc[:, ['constraint_id', 'unit', 'service']]
    variable_mapping_energy = constraints.loc[:, ['constraint_id', 'unit', 'service']]
    variable_mapping_energy['service'] = 'energy'
    variable_mapping = pd.concat([variable_mapping_reg, variable_mapping_energy])
    variable_mapping['coefficient'] = 1.0

    rhs_and_type = constraints.loc[:, ['unit', 'constraint_id', 'type', 'rhs']]

    return rhs_and_type, variable_mapping