Examples
==============
A number of examples of how to use Nempy are provided below. Examples 1 to 5 are simple and aim introduce various
market features that can be modelled with Nempy in an easy to understand way, the dispatch and pricing outcomes are
explained in inline comments where the results printed. Examples 6 and 7 show how to use the historical data input
preparation tools provided with Nempy to recreate historical dispatch intervals. Historical dispatch and pricing
outcomes can be difficult to interpret as they are usually the result of complex interactions between the many features
of the dispatch process, for these example the results are plotted in comparison to actual historical price outcomes.
Example 8 simply demonstrates how the outputs of one dispatch interval can be used as the initial conditions of the
next dispatch interval to create a time sequential model, additionally the current limitations with the approach are
briefly discussed.

1. Bid stack equivalent market
---------------------------
This example implements a one region bid stack model of an electricity market. Under the bid stack model, generators are
dispatched according to their bid prices, from cheapest to most expensive, until all demand is satisfied. No loss factors,
ramping constraints or other factors are considered.

.. literalinclude:: ../../examples/bidstack.py
    :linenos:
    :language: python


2. Unit loss factors, capacities and ramp rates
-----------------------------------------------
A simple example with two units in a one region market, units are given loss factors, capacity values and ramp rates.
The effects of loss factors on dispatch and market prices are explained.

.. literalinclude:: ../../examples/ramp_rates_and_loss_factors.py
    :linenos:
    :language: python


3. Interconnector with losses
-----------------------------
A simple example demonstrating how to implement a two region market with an interconnector. The interconnector is
modelled simply, with a fixed percentage of losses. To make the interconnector flow and loss calculation easy to
understand a single unit is modelled in the NSW region, NSW demand is set zero, and VIC region demand is set to 90 MW,
thus all the power to meet VIC demand must flow across the interconnetcor.

.. literalinclude:: ../../examples/interconnector_constant_loss_percentage.py
    :linenos:
    :language: python


4. Dynamic non-linear interconnector losses
----------------------------------------
Demonstrates how to model region demand dependant interconnector loss functions as decribed in the AEMO
:download:`Marginal Loss Factors documentation section 3 to 5  <../../docs/pdfs/Marginal Loss Factors for the 2020-21 Financial year.pdf>`.
To make the interconnector flow and loss calculation easy to understand a single unit is modelled in the NSW region,
NSW demand is set zero, and VIC region demand is set to 800 MW, thus all the power to meet VIC demand must flow across
the interconnetcor.


.. literalinclude:: ../../examples/interconnector_dynamic_losses.py
    :linenos:
    :language: python


5. Simple FCAS markets
----------------------------------------
Implements a market for energy, regulation raise and contingency 6 sec raise, with
co-optimisation constraints as described in section 6.2 and 6.3 of
:download:`FCAS Model in NEMDE <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

.. literalinclude:: ../../examples/simple_FCAS_markets.py
    :linenos:
    :language: python


6. Simple recreation of historical dispatch
----------------------------------------
Demonstrates using nempy to recreate historical dispatch intervals by implementing a simple energy market with unit bids,
unit maximum capacity constraints and interconnector models, all sourced from historical data published by AEMO.

To get graphs working install plotly.

.. image:: ../../examples/charts/energy_market_only_qld_prices.png
  :width: 600

*Results from example: for the QLD region a reasonable fit between modelled prices and historical prices is obtained.*

.. literalinclude:: ../../examples/recreating_historical_dispatch.py
    :linenos:
    :language: python

7. Detailed recreation of historical dispatch
------------------------------------------
Demonstrates using nempy to recreate historical dispatch intervals by implementing a simple energy market using all the
features of the nempy market model, all inputs sourced from historical data published by AEMO. Note each interval is
dispatched as a standalone simulation and the results from one dispatch interval are not carried over to be the initial
conditions of the next interval, rather the historical initial conditions are always used.

To get graphs working install plotly.

.. image:: ../../examples/charts/energy_market_only_qld_prices.png
  :width: 600

*Results from example: for the QLD region a very close fit between modelled prices and historical prices is obtained.*

.. literalinclude:: ../../examples/all_features_example.py
    :linenos:
    :language: python

8. Time sequential recreation of historical dispatch
-------------------------------------------------
Demonstrates using nempy to recreate historical dispatch in a dynamic or time sequential manner, this means the outputs
of one interval become the initial conditions for the next dispatch interval. Note, currently there is not the infrastructure
in place to include features such as generic constraints in the time sequential model as the rhs values of many constraints
would need to be re-calculated based on the dynamic system state. Similarly, using historical bids in this example is
some what problematic as participants also dynamically change their bids based on market conditions. However, for sake
of demonstrating how nempy can be used to create time sequential models, historical bids are used in this example.

.. literalinclude:: ../../examples/time_sequential.py
    :linenos:
    :language: python


