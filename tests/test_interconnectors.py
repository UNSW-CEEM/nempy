import pandas as pd
from nempy import interconnectors


def test_create_loss_function():
    # Interconnector flow
    flow = 300.0

    # Arbitrary demand inputs
    nsw_demand = 6000.0
    qld_demand = 7000.0
    demand = pd.DataFrame({
        'region': ['NSW1', 'QLD1'],
        'demand': [nsw_demand, qld_demand]
    })

    # Loss model details from 2020 Jan NEM web files.
    demand_coefficients = pd.DataFrame({
        'interconnector': ['NSW1-QLD1', 'NSW1-QLD1'],
        'region': ['NSW1', 'QLD1'],
        'demand_coefficient': [-0.00000035146, 0.000010044]
    })

    # Loss model details from 2020 Jan NEM web files.
    interconnector_coefficients = pd.DataFrame({
        'interconnector': ['NSW1-QLD1'],
        'loss_constant': [0.9529],
        'flow_coefficient': [0.00019617]
    })

    loss_function = interconnectors.create_loss_functions(interconnector_coefficients, demand_coefficients, demand)

    output_losses = loss_function['loss_function'].loc[0](flow)

    expected_losses = (-0.0471 - 3.5146E-07 * nsw_demand + 1.0044E-05 * qld_demand) * flow + 9.8083E-05 * flow ** 2

    assert(output_losses, expected_losses)
