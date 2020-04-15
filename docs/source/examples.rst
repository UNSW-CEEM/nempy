.. _Examples

Usage Examples
==============

Bid stack equivalent market
---------------------------
This example implements a one region market that mirrors the 'bid stack' model of an electricity market. Under the
bid stack model, generators are dispatched according to their bid prices, from cheapest to most expensive, until all
demand is satisfied. No loss factors, ramping constraints or other factors are considered.

.. literalinclude:: ../../examples/bidstack.py
    :linenos:
    :language: python


Ramp rates and loss factors
---------------------------
.. literalinclude:: ../../examples/ramp_rates_and_loss_factors.py
    :linenos:
    :language: python


