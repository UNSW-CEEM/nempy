---
title: 'Nempy: A Python package for modelling the Australian National Electricity Market dispatch procedure'

tags:
  - Python
  - electricity markets
  - economic dispatch
  - Australian National Electricity Market
  - NEM
  - dispatch
authors:
  - name: Nicholas Gorman
    affiliation: "1, 3"
  - name: Anna Bruce
    affiliation: "1, 3"
  - name: Iain MacGill
    affiliation: "2, 3"
affiliations:
 - name: School of Photovoltaics and Renewable Energy Engineering, University of New South Wales, Australia
   index: 1
 - name: School of Electrical Engineering and Telecommunications, University of New South Wales, Australia
   index: 2
 - name: Collaboration on Energy and Environmental Markets (CEEM), University of New South Wales, Australia
   index: 3
date: 16 August 2021
bibliography: paper.bib
---

# Summary

Nempy is a python package for modelling the dispatch procedure of the Australian National Electricity Market. Simple models can constructed using generator bids to supply electricity and demand for electricity. More complete models can be constructed by using inbuilt features to create mutiple market regions, ramp rate limits, loss factors, Frequency Control Ancillary Service (FCAS) markets, FCAS trapezium constraints, dynamic interconnector loss models, generic constraints and fast start dispatch inflexibility profiles. Outputs include market clearing prices, generator and scheduled load dispatch targets, FCAS enablement levels, unit FCAS availability levels, interconnector flows, interconnector losses and region net inflows. Nempy is written in Python 3, and uses a relatively small number of first order dependencies; pandas [@reback2020pandas; @mckinney-proc-scipy-2010], Numpy [@harris2020array], MIP-Python [@coin-orpython-mip], xmltodict [@xmltodict], and Requests [@psf].

# Statement of need

In modern industrialised economies, the electricity sector plays a key role in societal welfare and progress, yet commonly is also associated with major environmental harms, particularly where primary energy is sourced mainly through the burning of fossil fuels. As such, all of us are stakeholders in the continued successful operation of the electricity industry while it transitions to cleaner energy sources. Computer models are often used to study the operation, interactions and potential future direction of the electricity sector, review papers highlight the large body of work in this space [@ringkjob2018review; @chang2021trends; @fattahi2020systemic]. Such tools are, invariably, simplifications of the underlying processes of electricity industry operation and investment for reasons including the underlying complexity of the processes and the difficulty of gathering representative data. Commonly they tackle only a subset of the decision making which must operate from milliseconds (for example, under frequency relay trips) through to decades (investment in large generation units with long lead times). A particularly challenging and key task is that of operational dispatch – setting generator outputs and controllable network elements in order to meet expected demand over the next five to thirty minutes in order to minimize costs while ensuring secure and reliable operation. To the best of author's knowledge `Nempy` is the only open-source software that provides a detailed model of the Australian National Electricity Markets dispatch procedure. Other more generalized models of the NEM [@grozev2005nemsim; @mcconnell2013retrospective; @ANEMWorkingReport; @mountain; @wood] or commercial tools such as PLEXOS [@energy_exemplar_plexos_2021] and Prophet [@IES] are used to model NEM dispatch, at various levels of complexity, but are not open-source. More recent work by Xenophon and Hill provides open-source code and data for modeling the NEM, but the dispatch functionality does not include many of the NEM wholesale market features [@xenophon2018open].

# References