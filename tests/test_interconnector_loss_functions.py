import pytest
import pandas as pd
from nempy.historical_inputs import interconnectors


def test_create_loss_function():
    # Interconnector flow
    flow = 600.0

    # Arbitrary demand inputs
    nsw_demand = 7000.0
    qld_demand = 5000.0
    demand = pd.DataFrame({
        'region': ['NSW1', 'QLD1'],
        'loss_function_demand': [nsw_demand, qld_demand]
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
        'flow_coefficient': [0.00019617],
        'from_region_loss_share': [0.5]
    })

    loss_function = interconnectors.create_loss_functions(interconnector_coefficients,
                                                          demand_coefficients, demand)

    output_losses = loss_function['loss_function'].loc[0](flow)

    expected_losses = (-0.0471 - 3.5146E-07 * nsw_demand + 1.0044E-05 * qld_demand) * flow + 9.8083E-05 * flow ** 2

    assert(pytest.approx(expected_losses, 0.0001) == output_losses)


def test_create_loss_function_vic_nsw():
    # Interconnector flow
    flow = 600.0

    # Arbitrary demand inputs
    vic_demand = 6000.0
    nsw_demand = 7000.0
    sa_demand = 3000.0
    demand = pd.DataFrame({
        'region': ['NSW1', 'VIC1', 'SA1'],
        'loss_function_demand': [nsw_demand, vic_demand, sa_demand]
    })

    # Loss model details from 2020 Jan NEM web files.
    demand_coefficients = pd.DataFrame({
        'interconnector': ['VIC1-NSW1', 'VIC1-NSW1', 'VIC1-NSW1'],
        'region': ['NSW1', 'VIC1', 'SA1'],
        'demand_coefficient': [0.000021734, -0.000031523, -0.000065967]
    })

    # Loss model details from 2020 Jan NEM web files.
    interconnector_coefficients = pd.DataFrame({
        'interconnector': ['VIC1-NSW1'],
        'loss_constant': [1.0657],
        'flow_coefficient': [0.00017027],
        'from_region_loss_share': [0.5]
    })

    loss_function = interconnectors.create_loss_functions(interconnector_coefficients,
                                                          demand_coefficients, demand)

    output_losses = loss_function['loss_function'].loc[0](flow)

    expected_losses = (0.0657 - 3.1523E-05 * vic_demand + 2.1734E-05 * nsw_demand
                       - 6.5967E-05 * sa_demand) * flow + 8.5133E-05 * flow ** 2

    assert (pytest.approx(expected_losses, 0.0001) == output_losses)


def test_create_loss_function_bass_link():
    # Interconnector flow
    flow = -433.0

    # Arbitrary demand inputs
    nsw_demand = 6000.0
    qld_demand = 7000.0
    demand = pd.DataFrame({
        'region': ['VIC1', 'QLD1'],
        'loss_function_demand': [nsw_demand, qld_demand]
    })

    # Loss model details from 2020 Jan NEM web files.
    demand_coefficients = pd.DataFrame({
        'interconnector': ['BL'],
        'region': ['VIC1'],
        'demand_coefficient': [0.0]
    })

    # Loss model details from 2020 Jan NEM web files.
    interconnector_coefficients = pd.DataFrame({
        'interconnector': ['BL'],
        'loss_constant': [0.99608],
        'flow_coefficient': [0.00020786],
        'from_region_loss_share': [0.5]
    })

    loss_function = interconnectors.create_loss_functions(interconnector_coefficients,
                                                          demand_coefficients, demand)

    output_losses = loss_function['loss_function'].loc[0](flow)

    expected_losses = (-3.92E-3) * flow + (1.0393E-4) * flow ** 2

    assert(pytest.approx(expected_losses, 0.0001) == output_losses)