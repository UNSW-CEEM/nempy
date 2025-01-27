import pandas as pd
from pandas._testing import assert_frame_equal
from nempy.spot_market_backend import solver_interface


def test_dispatch():
    si = solver_interface.InterfaceToSolver()

    decision_variables = pd.DataFrame({
            'unit': ['A', 'A', 'B', 'B'],
            'upper_bound': [1, 6, 5, 7],
            'variable_id': [4, 5, 6, 7],
            'lower_bound': [0.0, 0.0, 0.0, 0.0],
            'type': ['continuous', 'continuous', 'continuous', 'continuous'],
    })

    si.add_variables(decision_variables)

    constraints_rhs_and_type = pd.DataFrame({
            'constraint_id': [0, 1],
            'type': ['<=', '<='],
            'rhs': [5, 15]
        })

    market_rhs_and_type = pd.DataFrame({
            'constraint_id': [2],
            'type': ['='],
            'rhs': [15]
    })

    rhs_and_type = pd.concat([constraints_rhs_and_type, market_rhs_and_type])

    constraints_lhs_coefficient = pd.DataFrame({
        'constraint_id': [0, 0, 1, 1, 2, 2, 2, 2],
        'variable_id': [4, 5, 6, 7, 4, 5, 6, 7],
        'coefficient': [1, 1, 1, 1, 1, 1, 1, 1]
    })

    si.add_constraints(constraints_lhs_coefficient, rhs_and_type)

    objective_function = pd.DataFrame({
            'variable_id': [4, 5, 6, 7],
            'cost': [0, 1, 2, 3]
    })

    si.add_objective_function(objective_function)

    si.optimize()

    si.linear_mip_model.optimize()

    decision_variables['value'] = si.get_optimal_values_of_decision_variables(decision_variables)

    prices = si.price_constraints([2])

    market_rhs_and_type['price'] = market_rhs_and_type['constraint_id'].map(prices)

    expected_decision_variables = pd.DataFrame({
            'unit': ['A', 'A', 'B', 'B'],
            'upper_bound': [1, 6, 5, 7],
            'variable_id': [4, 5, 6, 7],
            'lower_bound': [0.0, 0.0, 0.0, 0.0],
            'type': ['continuous', 'continuous', 'continuous', 'continuous'],
            'value': [1.0, 4.0, 5.0, 5.0]
    })

    expected_market_rhs_and_type = pd.DataFrame({
            'constraint_id': [2],
            'type': ['='],
            'rhs': [15],
            'price': [3.0]
    })

    assert_frame_equal(decision_variables, expected_decision_variables)
    assert_frame_equal(market_rhs_and_type, expected_market_rhs_and_type)
