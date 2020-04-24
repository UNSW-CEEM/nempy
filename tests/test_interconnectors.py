import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import markets


def test_create_interconnector_sos():
    def loss_function(loss):
        return 1 - loss

    break_points = pd.DataFrame({
        'segment_number': [1, 2, 3],
        'break_point': [-100, 0, 100]
    })


def test_interconnector_hard_code():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A'],
        '1': [120.0]  # MW
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A'],
        '1': [0.0]
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A'],
        'region': ['NSW']
    })

    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [0.0, 100.0]  # MW
    })

    simple_market = markets.Spot()
    simple_market.set_unit_info(unit_info)
    simple_market.set_unit_energy_volume_bids(volume_bids)
    simple_market.set_unit_energy_price_bids(price_bids)
    simple_market.set_demand_constraints(demand)

    nv = simple_market.next_variable_id
    nc = simple_market.next_constraint_id

    new_vars = pd.DataFrame({
        'variable_id': [nv, nv + 1],
        'lower_bound': [-120, -120],
        'upper_bound': [100, 100],
        'type': ['continuous', 'continuous']
    })

    simple_market.decision_variables['inter_flow'] = new_vars

    expected_demand_contribution = pd.DataFrame({
        'constraint_id': [0, 1],
        'variable_id': [nv, nv],
        'coefficient': [1.0, -1.0]
    })

    simple_market.constraints_lhs_coefficients['inter_flow'] = expected_demand_contribution

    expected_loss_contribution = pd.DataFrame({
        'constraint_id': [0, 1],
        'variable_id': [nv + 1, nv + 1],
        'coefficient': [0.0, -1.0]
    })

    simple_market.constraints_lhs_coefficients['inter_losses'] = expected_loss_contribution

    weights = pd.DataFrame({
        'variable_id': [nv + 2, nv + 3, nv + 4],
        'lower_bound': [0, 0, 0],
        'upper_bound': [1, 1, 1],
        'type': ['continuous', 'continuous', 'continuous']
    })

    simple_market.decision_variables['sos_one_weights'] = weights

    constraints_lhs = pd.DataFrame({
        'variable_id': [nv + 2, nv + 3, nv + 4, nv + 2, nv + 3, nv + 4, nv + 2, nv + 3, nv + 4],
        'constraint_id': [nc, nc, nc, nc + 1, nc + 1, nc + 1, nc + 2, nc + 2, nc + 2],
        'coefficient': [1, 1, 1, -120, 0, 100, 120 * 0.05, 0, 100 * 0.05]
    })

    simple_market.constraints_lhs_coefficients['interconnectors'] = constraints_lhs

    constraints_rhs = pd.DataFrame({
        'constraint_id': [nc],
        'rhs': [1],
        'type': ['='],
    })

    simple_market.constraints_rhs_and_type['interconnectors'] = constraints_rhs

    constraints_dynamic_rhs = pd.DataFrame({
        'constraint_id': [nc + 1, nc + 2],
        'rhs_variable_id': [nv, nv + 1],
        'type': ['=', '='],
    })

    simple_market.constraints_dynamic_rhs_and_type['interconnector'] = constraints_dynamic_rhs

    simple_market.dispatch()

    expected_variable_values = pd.DataFrame({
        'variable_id': [nv, nv + 1],
        'value': [- 100 / 0.95, (100 / 0.95) * 0.05]
    })

    assert_frame_equal(simple_market.decision_variables['inter_flow'].loc[:, ['variable_id', 'value']],
                       expected_variable_values)

    def test_create_interconnector_constraints():

        def losses(flow):
            return flow * 0.05

        break_points = pd.DataFrame({
            'interconnector_id': ['A', 'A', 'A', 'B', 'B', 'B'],
            'segment_number': [1, 2, 3, 1, 2, 3],
            'break_point': [-120, 0, 100, -50, 0, 80]
        })

        inter_directions = pd.DataFrame({
            'interconnector_id': ['A', 'B'],
            'to_region': ['NSW', 'VIC'],
            'from_region': ['QLD', 'QLD']
        })





