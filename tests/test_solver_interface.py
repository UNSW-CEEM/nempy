import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import solver_interface


def test_dispatch():
    decision_variables = {
        'energy_units': pd.DataFrame({
            'unit': ['A', 'A', 'B', 'B'],
            'upper_bound': [1, 6, 5, 7],
            'variable_id': [4, 5, 6, 7],
            'lower_bound': [0.0, 0.0, 0.0, 0.0],
            'type': ['continuous', 'continuous', 'continuous','continuous'],
        })
    }
    constraints_lhs_coefficient = {
        'unit_capacity': pd.DataFrame({
            'constraint_id': [0, 0, 1, 1],
            'variable_id': [4, 5, 6, 7],
            'coefficient': [1, 1, 1, 1]
        })
    }
    constraints_rhs_and_type = {'unit_capacity': pd.DataFrame({
        'constraint_id': [0, 1],
        'type': ['<=', '<='],
        'rhs': [5, 15]
    })}
    market_constraints_lhs_coefficients = {
        'energy_market': pd.DataFrame({
            'constraint_id': [2, 2, 2, 2],
            'variable_id': [4, 5, 6, 7],
            'coefficient': [1, 1, 1, 1]
        })
    }
    market_rhs_and_type = {
        'energy_market': pd.DataFrame({
            'constraint_id': [2],
            'type': ['='],
            'rhs': [15]
        })
    }
    objective_function = {
        'energy_bids': pd.DataFrame({
            'variable_id': [4, 5, 6, 7],
            'cost': [0, 1, 2, 3]
        })
    }
    split_decision_variables, market_rhs_and_type = solver_interface.dispatch(
        decision_variables, constraints_lhs_coefficient, constraints_rhs_and_type, market_constraints_lhs_coefficients,
        market_rhs_and_type, objective_function)
    expected_split_decision_variables = {
        'energy_units': pd.DataFrame({
            'unit': ['A', 'A', 'B', 'B'],
            'upper_bound': [1, 6, 5, 7],
            'variable_id': [4, 5, 6, 7],
            'lower_bound': [0.0, 0.0, 0.0, 0.0],
            'type': ['continuous', 'continuous', 'continuous','continuous'],
            'value': [1.0, 4.0, 5.0, 5.0]
        })
    }
    expected_market_rhs_and_type = {
        'energy_market': pd.DataFrame({
            'constraint_id': [2],
            'type': ['='],
            'rhs': [15],
            'price': [3.0]
        })
    }
    assert_frame_equal(split_decision_variables['energy_units'], expected_split_decision_variables['energy_units'])
    assert_frame_equal(market_rhs_and_type['energy_market'], expected_market_rhs_and_type['energy_market'])
