Introduction
============
The nempy package provides tools for modelling Australia's National Electricity Market (NEM) dispatch procedure. It
allows the user to provide data defining the market for a particular dispatch interval, then formulates the market
as a linear program, solves the problem to find the least cost dispatch for the market, and returns the dispatch of
each unit and market prices. Currently the package is being actively developed, the features already included are
volume and price bids from generators, generator capacity and ramp rate constraints, loss factors and regional energy
markets. Planned features included interconnectors, FCAS markets, load dispatch and generic network constraints. A
minimal worked example is shown below.

Minimal worked example
-----------------------
.. literalinclude:: ../../examples/bidstack.py
    :linenos:
    :language: python