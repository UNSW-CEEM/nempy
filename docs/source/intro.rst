Introduction
============
The nempy package provides tools for modelling Australia's National Electricity Market (NEM) dispatch procedure. It
allows the user to provide data defining the market for a particular dispatch interval, then formulates the market
as a linear program, solves the problem to find the least cost dispatch for the market, and returns the dispatch of
each unit and market prices.

Currently the package is being actively developed, the features already included are volume and price bids from generators
and loads, capacity and ramp rate constraints, loss factors, regional energy and FCAS markets, interconnectors and
generic constraints. Additionally, a module has been developed for downloading data from the Australian Energy Market
Operator's (AEMO) NEMWeb data portal and preprocessing this data for compatibility with the nempy SpotMarket class.

Note
----
nempy is still in the initial stages of development and no stable version has been released yet.

The next priorities for development are:
 - adding several features to the historical inputs module to allow
   for time sequential recreation of historical dispatch, primarily
   this involves adding support for preprocessing historical STPASA
   constraints
 - creating a bidding module that allows simple participant behaviour
   to be modelled


