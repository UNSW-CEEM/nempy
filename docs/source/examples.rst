Examples
==============

Bid stack equivalent market
---------------------------
This example implements a one region market that mirrors the 'bid stack' model of an electricity market. Under the
bid stack model, generators are dispatched according to their bid prices, from cheapest to most expensive, until all
demand is satisfied. No loss factors, ramping constraints or other factors are considered.

.. literalinclude:: ../../examples/bidstack.py
    :linenos:
    :language: python


Unit loss factors, capacities and ramp rates
--------------------------------------------
In this example units are given loss factors, capacity values and ramp rates.

.. literalinclude:: ../../examples/ramp_rates_and_loss_factors.py
    :linenos:
    :language: python


Interconnector with losses
---------------------------
.. literalinclude:: ../../examples/interconnector_constant_loss_percentage.py
    :linenos:
    :language: python


Dynamic non-linear interconnector losses
----------------------------------------
Implements creating loss functions as described in
:download:`Marginal Loss Factors documentation section 3 to 5  <../../docs/pdfs/Marginal Loss Factors for the 2020-21 Financial year.pdf>`.

.. literalinclude:: ../../examples/interconnector_dynamic_losses.py
    :linenos:
    :language: python


Simple FCAS markets
----------------------------------------
Implements a market for energy, regulation raise and contingency 6 sec raise, with
co-optimisation constraints as described in section 6.2 and 6.3 of
:download:`FCAS Model in NEMDE <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

.. literalinclude:: ../../examples/simple_FCAS_Markets.py
    :linenos:
    :language: python


