import pandas as pd


def test_create_interconnector_sos():
    def loss_function(loss):
        return 1 - loss

    break_points = pd.DataFrame({
        'segment_number': [1, 2, 3],
        'break_point': [-100, 0, 100]
    })


def test_simple_interconnector():

    interconnectors = pd.DataFrame({
        'interconnector_id'
        'to_region': ['NSW'],
        'from_region': ['VIC'],
        'loss_percentage': [0.05],
        'from_region_loss_share': [0.5],
        'max_flow': [100],
        'min_flow': [-120]
    })

    demand_constraints = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'constraint_id': [1, 3]
    })

    next_variable_id = 5
    next_constraint_id = 5

    new_vars = pd.DataFrame({
        'variable_id': [5, 6],
        'lower_bound': [-120, 0],
        'upper_bound': [100, 120],
        'type': ['continuous', 'continuous']
    })

    expected_demand_contribution = pd.DataFrame({
        'constraint_id': [1, 3],
        'variable_id': [5, 5],
        'coefficient': [1, -1]
    })

    expected_loss_contribution = pd.DataFrame({
        'constraint_id': [1, 3],
        'variable_id': [6, 6],
        'coefficient': [-0.5, -0.5]
    })

    weights = pd.DataFrame({
        'variable_id': [7, 8],
        'lower_bound': [0, 0],
        'upper_bound': [1, 1],
        'type': ['binary', 'binary']
    })

    constraints_lhs = pd.DataFrame({
        'variable_id': [7, 8, 7, 8, 7, 8],
        'constraint_id': [5, 5, 6, 6, 7, 7],
        'coefficient': [1, 1, -1, 1, 1, 1]
    })

    constraints_rhs = pd.DataFrame({
        'constraint_id': [5],
        'rhs': [1],
        'type': ['='],
    })

    constraints_dynamic_rhs = pd.DataFrame({
        'constraint_id': [6, 7],
        'rhs': [5, 6],
        'type': ['=', '='],
    })