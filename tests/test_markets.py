import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import markets


def test_one_region_energy_market():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [20.0, 20.0],  # MW
        '2': [50.0, 30.0],  # MW
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 52.0],  # $/MW
        '2': [53.0, 60.0],  # $/MW
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [55.0, 90.0],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW'],  # MW
        'loss_factor': [0.9, 0.95]  # MW/h
    })

    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [60.0]  # MW
    })

    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_bid_capacity_constraints(unit_limits)
    market.set_unit_price_bids(price_bids)
    market.set_demand_constraints(demand)
    market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [53 / 0.9]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'generator'],
        'service': ['energy', 'energy'],
        'dispatch': [40.0, 20.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_prices)
    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)


def test_two_region_energy_market():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [20.0, 20.0],  # MW
        '2': [50.0, 100.0],  # MW
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 52.0],  # $/MW
        '2': [53.0, 60.0],  # $/MW
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [70.0, 120.0],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'VIC'],  # MW
        'loss_factor': [0.9, 0.95]  # MW/h
    })

    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [60.0, 80.0]  # MW
    })

    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW', 'VIC'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_bid_capacity_constraints(unit_limits)
    market.set_unit_price_bids(price_bids)
    market.set_demand_constraints(demand)
    market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'price': [53 / 0.9, 60 / 0.95]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'generator'],
        'service': ['energy', 'energy'],
        'dispatch': [60.0, 80.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_prices)
    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)


def test_one_interconnector():
    # The only generator is located in NSW.
    unit_info = pd.DataFrame({
        'unit': ['A'],
        'region': ['NSW']  # MW
    })

    # Create a market instance.
    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW', 'VIC'])

    # Volume of each bids.
    volume_bids = pd.DataFrame({
        'unit': ['A'],
        '1': [100.0]  # MW
    })

    market.set_unit_volume_bids(volume_bids)

    # Price of each bid.
    price_bids = pd.DataFrame({
        'unit': ['A'],
        '1': [50.0]  # $/MW
    })

    market.set_unit_price_bids(price_bids)

    # NSW has no demand but VIC has 90 MW.
    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [0.0, 90.0]  # MW
    })

    market.set_demand_constraints(demand)

    # There is one interconnector between NSW and VIC. Its nominal direction is towards VIC.
    interconnectors = pd.DataFrame({
        'interconnector': ['little_link'],
        'to_region': ['VIC'],
        'from_region': ['NSW'],
        'max': [100.0],
        'min': [-120.0]
    })

    market.set_interconnectors(interconnectors)

    # The interconnector loss function. In this case losses are always 5 % of line flow.
    def constant_losses(flow):
        return abs(flow) * 0.05

    # The loss function on a per interconnector basis. Also details how the losses should be proportioned to the
    # connected regions.
    loss_functions = pd.DataFrame({
        'interconnector': ['little_link'],
        'from_region_loss_share': [0.5],  # losses are shared equally.
        'loss_function': [constant_losses]
    })

    # The points to linearly interpolate the loss function bewtween. In this example the loss function is linear so only
    # three points are needed, but if a non linear loss function was used then more points would be better.
    interpolation_break_points = pd.DataFrame({
        'interconnector': ['little_link', 'little_link', 'little_link'],
        'loss_segment': [1, 2, 3],
        'break_point': [-120.0, 0.0, 100]
    })

    market.set_interconnector_losses(loss_functions, interpolation_break_points)

    # Calculate dispatch.
    market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'price': [50., 50. * (((90. / 0.975) + (90. / 0.975) * 0.025) / 90)]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A'],
        'dispatch_type': ['generator'],
        'service': ['energy'],
        'dispatch': [(90.0 / 0.975) + (90.0 / 0.975) * 0.025]
    })

    expected_interconnector_flow = pd.DataFrame({
        'interconnector': ['little_link'],
        'link': ['little_link'],
        'flow': [90.0/0.975],
        'losses': [(90.0/0.975) * 0.05]
    })

    assert_frame_equal(market.get_energy_prices(), expected_prices)
    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)
    assert_frame_equal(market.get_interconnector_flows(), expected_interconnector_flow)


def test_one_region_energy_and_raise_regulation_markets():
    # Volume of each bid, number of bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B', 'B'],
        'service': ['energy', 'energy', 'raise_reg'],
        '1': [100.0, 110.0, 15.0],  # MW
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B', 'B'],
        'service': ['energy', 'energy', 'raise_reg'],
        '1': [50.0, 60.0, 20.0],  # $/MW
    })

    # Participant defined operational constraints on FCAS enablement.
    fcas_trapeziums = pd.DataFrame({
        'unit': ['B'],
        'service': ['raise_reg'],
        'max_availability': [15.0],
        'enablement_min': [50.0],
        'low_break_point': [65.0],
        'high_break_point': [95.0],
        'enablement_max': [110.0]
    })

    # Unit locations.
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW']
    })

    # The demand in the region\s being dispatched.
    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [100.0]  # MW
    })

    # FCAS requirement in the region\s being dispatched.
    fcas_requirement = pd.DataFrame({
        'set': ['nsw_regulation_requirement'],
        'region': ['NSW'],
        'service': ['raise_reg'],
        'volume': [10.0]  # MW
    })

    # Create the market model
    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)
    market.set_fcas_max_availability(
        fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']])
    market.set_energy_and_regulation_capacity_constraints(fcas_trapeziums)
    market.set_demand_constraints(demand)
    market.set_fcas_requirements_constraints(fcas_requirement)

    # Calculate dispatch and pricing
    market.dispatch()

    # Return the total dispatch of each unit in MW. Note that despite the energy bid of A being cheaper and capable
    # of meeting total demand, the more expensive B is dispatch up to 60 MW so that it can deliver the raise_reg
    # service.
    print(market.get_unit_dispatch())
    #   unit    service  dispatch
    # 0    A     energy      40.0
    # 1    B     energy      60.0
    # 2    B  raise_reg      10.0

    # Return the price of energy.
    print(market.get_energy_prices())
    #   region  price
    # 0    NSW   60.0

    # Return the price of regulation FCAS. Note to meet marginal FCAS demand unit B has to turn up and unit A needs to
    # turn down, at a net marginal cost of 10 $/MW, it would also cost 20 $/MW to increase unit B FCAS dispatch, hence
    # the total marginal cost of raise reg is 10 + 20 = 30.
    print(market.get_fcas_prices())
    #                           set  price
    # 0  nsw_regulation_requirement   30.0

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B', 'B'],
        'dispatch_type': ['generator', 'generator', 'generator'],
        'service': ['energy', 'energy', 'raise_reg'],
        'dispatch': [40.0, 60.0, 10.0]
    })

    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)

    expected_energy_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [50.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_energy_prices)

    expected_fcas_prices = pd.DataFrame({
        'region': ['NSW'],
        'service': ['raise_reg'],
        'price': [30.0]
    })

    assert_frame_equal(market.get_fcas_prices(), expected_fcas_prices)


def test_raise_6s_and_raise_reg():
    # Volume of each bid.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'service': ['energy', 'raise_6s', 'energy', 'raise_6s', 'raise_reg'],
        '1': [100.0, 10.0, 110.0, 15.0, 15.0],  # MW
    })

    # Price of each bid.
    price_bids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'service': ['energy', 'raise_6s', 'energy', 'raise_6s', 'raise_reg'],
        '1': [50.0, 35.0, 60.0, 20.0, 30.0],  # $/MW
    })

    # Participant defined operational constraints on FCAS enablement.
    fcas_trapeziums = pd.DataFrame({
        'unit': ['B', 'B', 'A'],
        'service': ['raise_reg', 'raise_6s', 'raise_6s'],
        'max_availability': [15.0, 15.0, 10.0],
        'enablement_min': [50.0, 50.0, 70.0],
        'low_break_point': [65.0, 65.0, 80.0],
        'high_break_point': [95.0, 95.0, 100.0],
        'enablement_max': [110.0, 110.0, 110.0]
    })

    # Unit locations.
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW']
    })

    # The demand in the region\s being dispatched.
    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [195.0]  # MW
    })

    # FCAS requirement in the region\s being dispatched.
    fcas_requirements = pd.DataFrame({
        'set': ['nsw_regulation_requirement', 'nsw_raise_6s_requirement'],
        'region': ['NSW', 'NSW'],
        'service': ['raise_reg', 'raise_6s'],
        'volume': [10.0, 10.0]  # MW
    })

    # Create the market model with unit service bids.
    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)

    # Create constraints that enforce the top of the FCAS trapezium.
    fcas_availability = fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]
    market.set_fcas_max_availability(fcas_availability)

    # Create constraints the enforce the lower and upper slope of the FCAS regulation
    # service trapeziums.
    regulation_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'raise_reg']
    market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)

    # Create constraints that enforce the lower and upper slope of the FCAS contingency
    # trapezium. These constrains also scale slopes of the trapezium to ensure the
    # co-dispatch of contingency and regulation services is technically feasible.
    contingency_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'raise_6s']
    market.set_joint_capacity_constraints(contingency_trapeziums)

    # Set the demand for energy.
    market.set_demand_constraints(demand)

    # Set the required volume of FCAS services.
    market.set_fcas_requirements_constraints(fcas_requirements)

    # Calculate dispatch and pricing
    market.dispatch()

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'dispatch_type': ['generator', 'generator', 'generator', 'generator', 'generator'],
        'service': ['energy', 'raise_6s', 'energy', 'raise_6s', 'raise_reg'],
        'dispatch': [100.0, 5.0, 95.0, 5.0, 10.0]
    })

    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)

    expected_energy_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [75.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_energy_prices)

    expected_fcas_prices = pd.DataFrame({
        'region': ['NSW', 'NSW'],
        'service': ['raise_6s', 'raise_reg'],
        'price': [35.0, 45.0]
    })

    assert_frame_equal(market.get_fcas_prices(), expected_fcas_prices)


def test_raise_1s_and_raise_reg():
    # Volume of each bid.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'service': ['energy', 'raise_1s', 'energy', 'raise_1s', 'raise_reg'],
        '1': [100.0, 10.0, 110.0, 15.0, 15.0],  # MW
    })

    # Price of each bid.
    price_bids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'service': ['energy', 'raise_1s', 'energy', 'raise_1s', 'raise_reg'],
        '1': [50.0, 35.0, 60.0, 20.0, 30.0],  # $/MW
    })

    # Participant defined operational constraints on FCAS enablement.
    fcas_trapeziums = pd.DataFrame({
        'unit': ['B', 'B', 'A'],
        'service': ['raise_reg', 'raise_1s', 'raise_1s'],
        'max_availability': [15.0, 15.0, 10.0],
        'enablement_min': [50.0, 50.0, 70.0],
        'low_break_point': [65.0, 65.0, 80.0],
        'high_break_point': [95.0, 95.0, 100.0],
        'enablement_max': [110.0, 110.0, 110.0]
    })

    # Unit locations.
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW']
    })

    # The demand in the region\s being dispatched.
    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [195.0]  # MW
    })

    # FCAS requirement in the region\s being dispatched.
    fcas_requirements = pd.DataFrame({
        'set': ['nsw_regulation_requirement', 'nsw_raise_1s_requirement'],
        'region': ['NSW', 'NSW'],
        'service': ['raise_reg', 'raise_1s'],
        'volume': [10.0, 10.0]  # MW
    })

    # Create the market model with unit service bids.
    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)

    # Create constraints that enforce the top of the FCAS trapezium.
    fcas_availability = fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]
    market.set_fcas_max_availability(fcas_availability)

    # Create constraints the enforce the lower and upper slope of the FCAS regulation
    # service trapeziums.
    regulation_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'raise_reg']
    market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)

    # Create constraints that enforce the lower and upper slope of the FCAS contingency
    # trapezium. These constrains also scale slopes of the trapezium to ensure the
    # co-dispatch of contingency and regulation services is technically feasible.
    contingency_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'raise_1s']
    market.set_joint_capacity_constraints(contingency_trapeziums)

    # Set the demand for energy.
    market.set_demand_constraints(demand)

    # Set the required volume of FCAS services.
    market.set_fcas_requirements_constraints(fcas_requirements)

    # Calculate dispatch and pricing
    market.dispatch()

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'dispatch_type': ['generator', 'generator', 'generator', 'generator', 'generator'],
        'service': ['energy', 'raise_1s', 'energy', 'raise_1s', 'raise_reg'],
        'dispatch': [100.0, 5.0, 95.0, 5.0, 10.0]
    })

    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)

    expected_energy_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [75.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_energy_prices)

    expected_fcas_prices = pd.DataFrame({
        'region': ['NSW', 'NSW'],
        'service': ['raise_1s', 'raise_reg'],
        'price': [35.0, 45.0]
    })

    assert_frame_equal(market.get_fcas_prices(), expected_fcas_prices)


def test_lower_1s_and_lower_reg():
    # Volume of each bid.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'service': ['energy', 'lower_1s', 'energy', 'lower_1s', 'lower_reg'],
        '1': [100.0, 10.0, 110.0, 15.0, 15.0],  # MW
    })

    # Price of each bid.
    price_bids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'service': ['energy', 'lower_1s', 'energy', 'lower_1s', 'lower_reg'],
        '1': [50.0, 35.0, 60.0, 20.0, 30.0],  # $/MW
    })

    # Participant defined operational constraints on FCAS enablement.
    fcas_trapeziums = pd.DataFrame({
        'unit': ['B', 'B', 'A'],
        'service': ['lower_reg', 'lower_1s', 'lower_1s'],
        'max_availability': [15.0, 15.0, 10.0],
        'enablement_min': [50.0, 50.0, 70.0],
        'low_break_point': [65.0, 65.0, 80.0],
        'high_break_point': [95.0, 95.0, 100.0],
        'enablement_max': [110.0, 110.0, 110.0]
    })

    # Unit locations.
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW']
    })

    # The demand in the region\s being dispatched.
    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [195.0]  # MW
    })

    # FCAS requirement in the region\s being dispatched.
    fcas_requirements = pd.DataFrame({
        'set': ['nsw_regulation_requirement', 'nsw_lower_1s_requirement'],
        'region': ['NSW', 'NSW'],
        'service': ['lower_reg', 'lower_1s'],
        'volume': [10.0, 10.0]  # MW
    })

    # Create the market model with unit service bids.
    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)

    # Create constraints that enforce the top of the FCAS trapezium.
    fcas_availability = fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]
    market.set_fcas_max_availability(fcas_availability)

    # Create constraints the enforce the lower and upper slope of the FCAS regulation
    # service trapeziums.
    regulation_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'lower_reg']
    market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)

    # Create constraints that enforce the lower and upper slope of the FCAS contingency
    # trapezium. These constrains also scale slopes of the trapezium to ensure the
    # co-dispatch of contingency and regulation services is technically feasible.
    contingency_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'lower_1s']
    market.set_joint_capacity_constraints(contingency_trapeziums)

    # Set the demand for energy.
    market.set_demand_constraints(demand)

    # Set the required volume of FCAS services.
    market.set_fcas_requirements_constraints(fcas_requirements)

    # Calculate dispatch and pricing
    market.dispatch()

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'dispatch_type': ['generator', 'generator', 'generator', 'generator', 'generator'],
        'service': ['energy', 'lower_1s', 'energy', 'lower_1s', 'lower_reg'],
        'dispatch': [100.0, 0.0, 95.0, 10.0, 10.0]
    })

    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)

    expected_energy_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [60.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_energy_prices)

    expected_fcas_prices = pd.DataFrame({
        'region': ['NSW', 'NSW'],
        'service': ['lower_1s', 'lower_reg'],
        'price': [20.0, 30.0]
    })

    assert_frame_equal(market.get_fcas_prices(), expected_fcas_prices)


def test_two_region_energy_market_with_regional_generic_constraints():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [100.0, 100.0]
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 20.0]
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [100.0, 120.0],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'VIC']
    })

    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [60.0, 80.0]  # MW
    })

    # Generic constraints
    generic_cons = pd.DataFrame({
        'set': ['X'],
        'type': ['>='],
        'rhs': [65.0],
    })

    region_coefficients = pd.DataFrame({
        'set': ['X'],
        'region': ['NSW'],
        'service': ['energy'],
        'coefficient': [1.0]
    })

    # There is one interconnector between NSW and VIC. Its nominal direction is towards VIC.
    interconnectors = pd.DataFrame({
        'interconnector': ['little_link'],
        'to_region': ['VIC'],
        'from_region': ['NSW'],
        'max': [100.0],
        'min': [-120.0]
    })

    market = markets.SpotMarket(unit_info=unit_info, market_regions=['VIC', 'NSW'])
    market.set_interconnectors(interconnectors)
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_bid_capacity_constraints(unit_limits)
    market.set_unit_price_bids(price_bids)
    market.set_demand_constraints(demand)
    market.set_generic_constraints(generic_cons)
    market.link_regions_to_generic_constraints(region_coefficients)
    market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'price': [20.0, 20.0]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'generator'],
        'service': ['energy', 'energy'],
        'dispatch': [65.0, 75.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_prices)
    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)


def test_two_region_energy_market_with_unit_generic_constraints():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [100.0, 100.0]
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 20.0]
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [100.0, 120.0],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'VIC']
    })

    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [60.0, 80.0]  # MW
    })

    # Generic constraints
    generic_cons = pd.DataFrame({
        'set': ['X'],
        'type': ['>='],
        'rhs': [65.0],
    })

    unit_coefficients = pd.DataFrame({
        'set': ['X'],
        'unit': ['A'],
        'service': ['energy'],
        'coefficient': [1.0]
    })

    # There is one interconnector between NSW and VIC. Its nominal direction is towards VIC.
    interconnectors = pd.DataFrame({
        'interconnector': ['little_link'],
        'to_region': ['VIC'],
        'from_region': ['NSW'],
        'max': [100.0],
        'min': [-120.0]
    })

    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW', 'VIC'])
    market.set_interconnectors(interconnectors)
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_bid_capacity_constraints(unit_limits)
    market.set_unit_price_bids(price_bids)
    market.set_demand_constraints(demand)
    market.set_generic_constraints(generic_cons)
    market.link_units_to_generic_constraints(unit_coefficients)
    market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'price': [20.0, 20.0]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'generator'],
        'service': ['energy', 'energy'],
        'dispatch': [65.0, 75.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_prices)
    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)


def test_two_region_energy_market_with_interconnector_generic_constraints():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [100.0, 100.0]
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 20.0]
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [100.0, 120.0],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'VIC']
    })

    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [60.0, 80.0]  # MW
    })

    # Generic constraints
    generic_cons = pd.DataFrame({
        'set': ['X'],
        'type': ['>='],
        'rhs': [10.0],
    })

    interconnector_coefficients = pd.DataFrame({
        'set': ['X'],
        'interconnector': ['little_link'],
        'coefficient': [1.0]
    })

    # There is one interconnector between NSW and VIC. Its nominal direction is towards VIC.
    interconnectors = pd.DataFrame({
        'interconnector': ['little_link'],
        'to_region': ['VIC'],
        'from_region': ['NSW'],
        'max': [100.0],
        'min': [-120.0]
    })

    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW', 'VIC'])
    market.set_interconnectors(interconnectors)
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_bid_capacity_constraints(unit_limits)
    market.set_unit_price_bids(price_bids)
    market.set_demand_constraints(demand)
    market.set_generic_constraints(generic_cons)
    market.link_interconnectors_to_generic_constraints(interconnector_coefficients)
    market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'price': [50.0, 20.0]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'generator'],
        'service': ['energy', 'energy'],
        'dispatch': [70.0, 70.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_prices)
    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)


def test_use_unit_generic_constraints_to_exclude_unit_from_providing_raise_reg():
    # Volume of each bid.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'service': ['energy', 'raise_6s', 'energy', 'raise_6s', 'raise_reg'],
        '1': [100.0, 11.0, 110.0, 15.0, 15.0],  # MW
    })

    # Price of each bid.
    price_bids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'service': ['energy', 'raise_6s', 'energy', 'raise_6s', 'raise_reg'],
        '1': [50.0, 35.0, 60.0, 20.0, 30.0],  # $/MW
    })

    # Unit locations.
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW']
    })

    # The demand in the region\s being dispatched.
    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [195.0]  # MW
    })

    # FCAS requirement in the region\s being dispatched.
    fcas_requirements = pd.DataFrame({
        'set': ['nsw_regulation_requirement', 'nsw_raise_6s_requirement'],
        'region': ['NSW', 'NSW'],
        'service': ['raise_reg', 'raise_6s'],
        'volume': [10.0, 10.0]  # MW
    })

    # Generic constraints
    interconnector_coefficients = pd.DataFrame({
        'set': ['nsw_raise_6s_requirement'],
        'unit': ['B'],
        'service': ['raise_6s'],
        'coefficient': [-1.0],
    })

    # Create the market model with unit service bids.
    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW', 'VIC'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)

    # Set the demand for energy.
    market.set_demand_constraints(demand)

    # Set the required volume of FCAS services.
    market.set_fcas_requirements_constraints(fcas_requirements)

    # Create generic constraints
    market.link_units_to_generic_constraints(interconnector_coefficients)

    # Calculate dispatch and pricing
    market.dispatch()

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B', 'B'],
        'dispatch_type': ['generator', 'generator', 'generator', 'generator', 'generator'],
        'service': ['energy', 'raise_6s', 'energy', 'raise_6s', 'raise_reg'],
        'dispatch': [100.0, 10.0, 95.0, 0.0, 10.0]
    })

    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)


def test_one_region_energy_market_with_elastic_unit_generic_constraints():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [100.0, 100.0]
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 20.0]
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [100.0, 120.0],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW']
    })

    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [80.0]  # MW
    })

    # Generic constraints
    generic_cons = pd.DataFrame({
        'set': ['X'],
        'type': ['>='],
        'rhs': [65.0],
    })

    violation_costs = pd.DataFrame({
        'set': ['X'],
        'cost': [1000.0]
    })

    unit_coefficients = pd.DataFrame({
        'set': ['X'],
        'unit': ['A'],
        'service': ['energy'],
        'coefficient': [1.0]
    })

    market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_bid_capacity_constraints(unit_limits)
    market.set_unit_price_bids(price_bids)
    market.set_demand_constraints(demand)
    market.set_generic_constraints(generic_cons)
    market.make_constraints_elastic('generic', violation_costs)
    market.link_units_to_generic_constraints(unit_coefficients)
    market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [20.0]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'generator'],
        'service': ['energy', 'energy'],
        'dispatch': [65.0, 15.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_prices)
    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)


def test_setting_constraint_on_unit_with_no_bid_volume_doesnt_raise_error():
    # Unit 'C' is on outage, energy bids all 0
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B', 'C'],
        '1': [20.0, 50.0, 0.0],
        '2': [20.0, 30.0, 0.0],
        '3': [5.0, 10.0, 0.0]
    })

    price_bids = pd.DataFrame({
        'unit': ['A', 'B', 'C'],
        '1': [50.0, 50.0, 0.0],
        '2': [60.0, 55.0, 0.0],
        '3': [100.0, 80.0, 0.0]
    })

    unit_info = pd.DataFrame({
        'unit': ['A', 'B', 'C'],
        'region': ['NSW', 'NSW', 'NSW'],
    })

    # Max capacity also set to 0 for unit 'C'
    max_capacity = pd.DataFrame({
        'unit': ['A', 'B', 'C'],
        'capacity': [50.0, 100.0, 0.0],
    })

    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [120.0]
    })

    market = markets.SpotMarket(unit_info=unit_info,
                                market_regions=['NSW'])
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)
    market.set_demand_constraints(demand)
    market.set_unit_bid_capacity_constraints(max_capacity)

    market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [60.0]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'generator'],
        'service': ['energy', 'energy'],
        'dispatch': [40.0, 80.0]
    })

    assert_frame_equal(market.get_energy_prices(), expected_prices)
    assert_frame_equal(market.get_unit_dispatch(), expected_dispatch)