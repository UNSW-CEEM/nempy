The Spot market class
===============================
A model of the NEM spot market dispatch process.

Overview
--------
The market, both in real life and in this model, is implemented as a linear program. Linear programs consist of three
elements:

1.  **Decision variables**: the quantities being optimised for. In an electricity market these will be things like the
    outputs of generators, the consumption of dispatchable loads and interconnector flows.
2.  An **objective function**: the linear function being optimised. In this model of the spot market the cost of production
    is being minimised, and is defined as the sum of each bids dispatch level multiplied by the bid price.
3.  A set of **linear constraints**: used to implement market features such as network constraints and interconnectors.

The class :func:`nempy.markets.Spot` is used to construct these elements and then solve the linear program to calculate
dispatch and pricing. The examples below give an overview of how method calls build the linear program.

*   Initialising the market instance, doesn't create any part of the linear program, just saves general information for
    later use.

.. code-block:: python

    simple_market = markets.Spot(unit_info=unit_info)

*   Providing volume bids creates a set of n decision variables, where n is the number of bids with a volume greater
    than zero.

.. code-block:: python

    simple_market.set_unit_energy_volume_bids(volume_bids)

*   Providing price bids creates the objective function, i.e. units will be dispatch to minimise cost, as determined
    by the bid prices.

.. code-block:: python

    simple_market.set_unit_energy_price_bids(price_bids)

*   Providing unit capacities creates a constraint for each unit that caps its total dispatch at a set capacity

.. code-block:: python

    simple_market.set_unit_capacity_constraints(unit_limits)

*   Providing regional energy demand creates a constraint for each region that forces supply from units and
    interconnectors to equal demand

.. code-block:: python

    simple_market.set_demand_constraints(demand)

Reference
---------
.. currentmodule:: nempy.markets

.. autosummary:: nempy.markets
    Spot
    Spot.__init__
    Spot.set_unit_energy_volume_bids
    Spot.set_unit_energy_price_bids
    Spot.set_unit_capacity_constraints
    Spot.set_unit_ramp_up_constraints
    Spot.set_unit_ramp_down_constraints
    Spot.set_demand_constraints
    Spot.dispatch
    Spot.get_energy_dispatch
    Spot.get_energy_prices


.. autoclass:: nempy.markets.Spot
    :members:



