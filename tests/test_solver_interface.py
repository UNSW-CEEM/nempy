import pandas as pd
from pandas._testing import assert_frame_equal
from nempy.spot_markert_backend import solver_interface


def test_dispatch():
    decision_variables = {
        'energy_units': pd.DataFrame({
            'unit': ['A', 'A', 'B', 'B'],
            'upper_bound': [1, 6, 5, 7],
            'variable_id': [4, 5, 6, 7],
            'lower_bound': [0.0, 0.0, 0.0, 0.0],
            'type': ['continuous', 'continuous', 'continuous', 'continuous'],
        })
    }
    constraints_rhs_and_type = {
            'capacity': pd.DataFrame({
                'constraint_id': [0, 1],
                'type': ['<=', '<='],
                'rhs': [5, 15]
        })
    }
    market_rhs_and_type = {
        'demand': pd.DataFrame({
            'constraint_id': [2],
            'type': ['='],
            'rhs': [15]
        })
    }
    constraints_lhs_coefficient = pd.DataFrame({
            'constraint_id': [0, 0, 1, 1, 2, 2, 2, 2],
            'variable_id': [4, 5, 6, 7, 4, 5, 6, 7],
            'coefficient': [1, 1, 1, 1, 1, 1, 1, 1]
    })
    objective_function = {
        'energy_bids': pd.DataFrame({
            'variable_id': [4, 5, 6, 7],
            'cost': [0, 1, 2, 3]
        })
    }
    constraints_dynamic_rhs_and_type = {}
    split_decision_variables, market_rhs_and_type = solver_interface.dispatch(
        decision_variables, constraints_lhs_coefficient, constraints_rhs_and_type, market_rhs_and_type,
        constraints_dynamic_rhs_and_type, objective_function)
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
        'demand': pd.DataFrame({
            'constraint_id': [2],
            'type': ['='],
            'rhs': [15],
            'price': [3.0]
        })
    }
    assert_frame_equal(split_decision_variables['energy_units'], expected_split_decision_variables['energy_units'])
    assert_frame_equal(market_rhs_and_type['demand'], expected_market_rhs_and_type['demand'])
