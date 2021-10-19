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

Nempy is a python package for modelling the dispatch procedure of the Australian National Electricity Market (NEM).
Electricity markets are a way of co-ordinating the supply of electricity by private firms. The NEM is a gross pool spot 
market that operates on 5 min dispatch basis. Described simply, this means all generators wishing to sell electricity 
must bid into the market every 5 minutes, market clearing proceeds by calculating the cheapest combination of generator 
operating levels to meet forecast demand at the end of 5 the minute dispatch interval. The price of electricity is set as the 
marginal cost of generation, which, under a simple market formulation, would be the cost of the next generation bid to be 
dispatched if demand for electricity were to increase. Real-world formulations require significant additional adjustment 
in order to manage the technical complexity of securely and reliably operating an electricity grid. For example, in the 
case of the NEM additional markets for ancillary services have been introduced. One set of ancillary markets that have 
been integrated into the market dispatch procedure are the Frequency Control Ancillary Services (FCAS) markets. In these 
markets generators compete to provide the ability to rapidly change generation levels in order to stabilise the grid frequency. 
Nempy is flexible in that it allows for formulation of very simple market models, for the formulation of market models 
of near real-world complexity, and at the various levels of intermediate complexity. Simple models can be constructed 
using just generator bids and electricity demand, so called bid stack models. More complete models can be constructed by 
using the inbuilt features to create multiple market regions, ramp rate limits, loss factors, FCAS markets, FCAS trapezium 
constraints, dynamic interconnector loss models, generic constraints and fast start dispatch inflexibility profiles. 
Outputs include market clearing prices, generator and scheduled load dispatch targets, FCAS enablement levels, unit FCAS 
availability levels, interconnector flows, interconnector losses and region net inflows. Nempy is written in Python 3, 
and uses a relatively small number of first-order dependencies; pandas [@reback2020pandas; @mckinney-proc-scipy-2010], 
Numpy [@harris2020array], MIP-Python [@coin-orpython-mip], xmltodict [@xmltodict], and Requests [@psf].

# Statement of need

In modern industrialised economies, the electricity sector plays a key role in societal welfare and progress, yet 
commonly is also associated with major environmental harms, particularly where primary energy is sourced mainly through 
the burning of fossil fuels. As such, all of us are stakeholders in the continued successful operation of the 
electricity industry, while it transitions to cleaner energy sources, and beyond. Computer models are often used to 
study the operation, interactions and potential future direction of the electricity sector, review papers highlight the 
large body of work in this space [@ringkjob2018review; @chang2021trends; @fattahi2020systemic]. Such tools are, 
invariably, simplifications of the underlying processes of electricity industry operation and investment for reasons 
including the underlying complexity of the processes and the difficulty of gathering representative data. Commonly they 
tackle only a subset of the decision making that must operate from milliseconds (for example, under frequency relay 
trips) through to decades (investment in large generation units with long lead times). A particularly challenging and 
key task is that of operational dispatch – setting generator outputs and controllable network elements to meet expected 
demand over the next five to thirty minutes, and minimising costs while ensuring secure and reliable operation. To the 
best of the author's knowledge `Nempy` is the only open-source software that provides a detailed model of the NEM's 
dispatch procedure. Other more generalised models of the NEM [@grozev2005nemsim; @mcconnell2013retrospective; 
@ANEMWorkingReport; @mountain; @wood] or commercial tools such as PLEXOS [@energy_exemplar_plexos_2021] and Prophet 
[@IES] are used to model NEM dispatch, at various levels of complexity, but are not open-source. More recent work by 
Xenophon and Hill provides open-source code and data for modelling the NEM, but the dispatch functionality does not 
include many of the NEM wholesale market features [@xenophon2018open].

# Use cases
Nempy has been designed as a flexible model of the NEM's dispatch procedure and to be re-usable in a number of 
contexts. Some potential use cases are outlined below:

1. As a tool for studying the dispatch process itself. Models of any energy system or electricity market are necessarily 
simplifications, however, to improve model performance it is often desirable to add additional detail. Nempy can be used 
to study the impact of different simplifications on modelling outcomes, and thus provide guidance on how model 
performance could be improved by adding additional detail. Figure 1 shows a simple example of such an analysis. The price
results from the New South Wales region for 1000 randomly selected intervals in the 2019 calender year are shown. When
Nempy is configured with a full set of market features price results closely match historical prices. When the FCAS 
markets and generic constraints (network and security) are removed from the model results differ significantly. Resorting
the results of the simpler market model, we can see that both models produce a similar number of medianly priced 
intervals. However, the highest and lowest priced intervals of the simpler model are significantly lower. The average
historical price is 81.4 $/MWh, the average price of the full featured model is 81.3 $/MWh, and the average price of the 
simpler model is 75 $/MWh. The close match between the results of the full featured model and historical prices allows 
for the attribution of the deviation of the simpler model explicitly to the simplification that have been made.  

![Dispatch price results from the New South Wales region for 1000 randomly selected intervals in the 2019 calender year.
  The historical prices, prior to scaling or capping are also shown for comparison. Results from two Nempy models are
  shown, one with a full set of dispatch features, and one without FCAS markets or generic constraints (network and 
  security constraints). For the simpler model price results are shown both historical price order and resorted.\label{fig:example}](plot.png)

2. As a building block in agent based market models. Agent based models can be used to study electricity market 
operation, and are particularly useful in modelling both the competitive nature of electricity markets and their complex 
operational constraints [@ventosa]. In such models, agents must interact with a modelled environment, and a key part of that 
environment is the market dispatch process. Thus, Nempy could be useful as a building block to create agent based models 
of the NEM, and play a role in answering various questions about market operational outcomes. Such questions could 
include: 

    * How does changing the demand for electricity effect market outcomes? 
    * How does the entry of new generating technologies effect market outcomes? 
    * How do patterns of generator ownership effect market outcomes? 

Of course, another necessary component of agent based models are the behavioural models of the agents, a prototype 
behavioural model of NEM participants is being developed as part of the NEMPRO project [@nempro].

3. To answer counter factual questions about historical dispatch outcomes. For example:

    * What would have been the impact on market dispatch if a particular network constraint had not been present? 
    * How would have dispatch outcomes differed if a unit had offered a different bid into the market? 

The answers to such questions have direct, and  potentially large, financial implications for market participants. 
AEMO offers access to a production version of the market dispatch engine to allow participants to answer such questions 
[@nemde]. However, access is restricted to registered participants and is provided at a cost of $15,000 per year. 
Additionally, users of this service are not provided with a copy of the dispatch engine, but access it by submitting 
input files to AEMO. This prevents the use of this service to answer questions about how changes to the dispatch 
process, rather than the inputs, would effect dispatch outcomes. In contrast, access to Nempy is not restricted, it is 
free to use, and is open to modification.

4. As a reference implementation of the NEM's dispatch procedure. While the Australian Energy Market Operator (AEMO) 
has published several documents that describe aspects of the dispatch process [@fcasmodel; @faststart; @lossfactors; 
@constraintviolation; @treatmentlossfactors], our experience developing Nempy has indicated that key 
implementation details are often missing from the publicly available documentation. Through a process of testing various 
implementation options, where the documentation was not explicit, Nempy has been refined in an attempt to better reflect 
the actual dispatch procedure. As a result Nempy is a useful additional reference for analysts and modelers 
looking to understand the NEM's dispatch procedure.

# References