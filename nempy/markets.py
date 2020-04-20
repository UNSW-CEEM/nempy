import numpy as np
from nempy import check, market_constraints, objective_function, solver_interface, unit_constraints, variable_ids

class Spot:
    """Class for constructing and dispatch the spot market on an interval basis."""
    def __init__(self, unit_info, dispatch_interval=5):
        """Initialises the spot market with general information required.

        Parameters
        ----------
        unit_info : pd.DataFrame
            Information on a unit basis, not all columns are required.

            ===========  ==============================================================================================
            Columns:     Description:
            unit         unique identifier of a dispatch unit, required (as `str`)
            region       location of unit, required (as `str`)
            loss_factor  marginal, average or combined loss factors, \n
                         :download:`see AEMO doc <../../docs/pdfs/Treatment_of_Loss_Factors_in_the_NEM.pdf>`, \n
                         optional (as `float`)
            ===========  ==============================================================================================

        dispatch_interval : int
            The length of the dispatch interval in minutes."""

        self.dispatch_interval = dispatch_interval
        self.unit_info = unit_info
        self.decision_variables = {}
        self.constraints_lhs_coefficients = {}
        self.constraints_rhs_and_type = {}
        self.market_constraints_lhs_coefficients = {}
        self.market_constraints_rhs_and_type = {}
        self.objective_function_components = {}
        self.next_variable_id = 0
        self.next_constraint_id = 0
        self.check = True

    @check.required_columns('volume_bids', ['unit'])
    @check.allowed_columns('volume_bids', ['unit', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    @check.repeated_rows('volume_bids', ['unit'])
    @check.column_data_types('volume_bids', {'unit': str, 'else': np.float64})
    @check.column_values('volume_bids', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    def set_unit_energy_volume_bids(self, volume_bids):
        """Creates the decision variables corresponding to energy bids.

        Variables are created by reserving a variable id (as `int`) for each bid. Bids with a volume of 0 MW do not
        have a variable created. The lower bound of the variables are set to zero and the upper bound to the bid
        volume, the variable type is set to continuous.

        Examples
        --------
        >>> import pandas as pd
        >>> from nempy import markets

        The bids for two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20, '50'],
        ...     '2': [20, 30],
        ...     '3': [5, 10]})

        Both units are in the same region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> simple_market = markets.Spot(unit_info)

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        The market should now have the variables.

        >>> print(simple_market.decision_variables['energy_bids'])
           variable_id unit capacity_band  lower_bound  upper_bound        type
        0            0    A             1          0.0           20  continuous
        1            1    A             2          0.0           20  continuous
        2            2    A             3          0.0            5  continuous
        3            3    B             1          0.0           50  continuous
        4            4    B             2          0.0           30  continuous
        5            5    B             3          0.0           10  continuous

        Parameters
        ----------
        volume_bids : pd.DataFrame
            Bids by unit, in MW, can contain up to 10 bid bands, these should be labeled '1' to '10'.

            ========  ======================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            1         bid volume in the 1st band, in MW (as `float`)
            2         bid volume in the 2nd band, in MW (as `float`)
            n         bid volume in the nth band, in MW (as `float`)
            ========  ======================================================

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the require type.
            MissingColumnError
                If the column 'units' is missing or there are no bid bands.
            UnexpectedColumn
                There is a column that is not 'units' or '1' to '10'
        """

        # Create unit variable ids
        self.decision_variables['energy_bids'] = variable_ids.energy(volume_bids, self.next_variable_id)
        # Update the variable id counter:
        self.next_variable_id = max(self.decision_variables['energy_bids']['variable_id']) + 1

    @check.energy_bid_ids_exist
    @check.repeated_rows('price_bids', ['unit'])
    def set_unit_energy_price_bids(self, price_bids):
        """
        Control layer method, handles the creation of objective function costs corresponding to energy bids.

        If no loss factors have been provided as part of the unit information when the model was initialised then the
        costs in the objective function are as bid. If loss factors are provided then the bid costs are referred to the
        regional reference node by dividing by the loss factor.

        The data manipulation is handled by the sub functions :func:`nempy.objective_function.energy` and
        :func:`nempy.objective_function.scale_by_loss_factors`.

        Parameters
        ----------
        price_bids : pd.DataFrame
            Bids by unit, in $/MW, can contain up to n bid bands.

            ========  ======================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            1         bid price in the 1st band, in $/MW (as `float`)
            2         bid price in the 2nd band, in $/MW (as `float`)
            n         bid price in the nth band, in $/MW (as `float`)
            ========  ======================================================

        Returns
        -------
        None
        """
        energy_objective_function = objective_function.energy(self.decision_variables['energy_bids'], price_bids)
        if 'loss_factor' in self.unit_info.columns:
            energy_objective_function = objective_function.scale_by_loss_factors(energy_objective_function,
                                                                                 self.unit_info)
        self.objective_function_components['energy_bids'] = energy_objective_function.loc[:, ['variable_id', 'cost']]

    @check.energy_bid_ids_exist
    @check.repeated_rows('unit_limits', ['unit'])
    def set_unit_capacity_constraints(self, unit_limits):
        """Control layer method, handles the implementation of the constraints that limit unit output based on capacity.

        1. Create the constraints: see unit_constraints.capacity docstring for details.
        2. Save constraint details.
        3. Update the constraint and variable id counter: the next available integer to be used as an id.

        :param unit_limits:
            unit: str
                The unique name of each unit
            capacity: float
                The maximum output of the unit if unconstrained by ramp rate, in MW
        :return:
        """
        # 1. Create the constraints
        lhs_coefficients, rhs_and_type = unit_constraints.capacity(self.decision_variables['energy_bids'], unit_limits,
                                                                   self.next_constraint_id)
        # 2. Save constraint details.
        self.constraints_lhs_coefficients['unit_capacity'] = lhs_coefficients
        self.constraints_rhs_and_type['unit_capacity'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(lhs_coefficients['constraint_id']) + 1

    @check.energy_bid_ids_exist
    @check.repeated_rows('unit_limits', ['unit'])
    def set_unit_ramp_up_constraints(self, unit_limits):
        """Control layer method, handles the implementation of constraints on unit output based on ramp up rate.

        1. Create the constraints: see unit_constraints.ramp_up docstring for details.
        2. Save constraint details.
        3. Update the constraint and variable id counter: the next available integer to be used as an id.

        :param unit_limits:
            unit: str
                The unique name of each unit
            ramp_up_rate: float
                The maximum rate at which the unit can increase output, in MW/h
        :return:
        """
        # 1. Create the constraints
        lhs_coefficients, rhs_and_type = unit_constraints.ramp_up(self.decision_variables['energy_bids'], unit_limits,
                                                                  self.next_constraint_id, self.dispatch_interval)
        # 2. Save constraint details.
        self.constraints_lhs_coefficients['ramp_up'] = lhs_coefficients
        self.constraints_rhs_and_type['ramp_up'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(lhs_coefficients['constraint_id']) + 1

    @check.energy_bid_ids_exist
    @check.repeated_rows('unit_limits', ['unit'])
    def set_unit_ramp_down_constraints(self, unit_limits):
        """Control layer method, handles the implementation of constraints on unit output based on ramp down rate.

        2. Create the constraints: see unit_constraints.ramp_up docstring for details.
        3. Save constraint details.
        4. Update the constraint and variable id counter: the next available integer to be used as an id.

        :param unit_limits:
            unit: str
                The unique name of each unit
            ramp_down_rate: float
                The maximum rate at which the unit can decrease output, in MW/h
        :return:
        """
        # 1. Create the constraints
        lhs_coefficients, rhs_and_type = unit_constraints.ramp_down(self.decision_variables['energy_bids'],
                                                                    unit_limits, self.next_constraint_id,
                                                                    self.dispatch_interval)
        # 2. Save constraint details.
        self.constraints_lhs_coefficients['ramp_down'] = lhs_coefficients
        self.constraints_rhs_and_type['ramp_down'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(lhs_coefficients['constraint_id']) + 1

    @check.energy_bid_ids_exist
    @check.repeated_rows('demand', ['region'])
    def set_demand_constraints(self, demand):
        """Control layer method, handles the implementation of the constraints that create the energy market.

        1. Create the constraints: see market_constraints.energy docstring for details.
        2. Save constraint details.
        3. Update the constraint id counter: the next available integer to be used as a constraint id.

        :param demand: DataFrame
            region: string
                The regions to create energy markets for.
            demand: float
                The demand in each region in MW.
        :return:
        """

        # 1. Create the constraints
        lhs_coefficients, rhs_and_type = market_constraints.energy(self.decision_variables['energy_bids'],
                                                                   demand, self.unit_info, self.next_constraint_id)
        # 2. Save constraint details
        self.market_constraints_lhs_coefficients['energy_market'] = lhs_coefficients
        self.market_constraints_rhs_and_type['energy_market'] = rhs_and_type
        # 3. Update the constraint id
        self.next_constraint_id = max(lhs_coefficients['constraint_id']) + 1

    @check.pre_dispatch
    def dispatch(self):
        decision_variables, market_constraints_rhs_and_type = solver_interface.dispatch(
            self.decision_variables, self.constraints_lhs_coefficients, self.constraints_rhs_and_type,
            self.market_constraints_lhs_coefficients, self.market_constraints_rhs_and_type,
            self.objective_function_components)
        self.market_constraints_rhs_and_type = market_constraints_rhs_and_type
        self.decision_variables = decision_variables

    def get_energy_dispatch(self):
        dispatch = self.decision_variables['energy_bids'].loc[:, ['unit', 'value']]
        dispatch.columns = ['unit', 'dispatch']
        return dispatch.groupby('unit', as_index=False).sum()

    def get_energy_prices(self):
        prices = self.market_constraints_rhs_and_type['energy_market'].loc[:, ['region', 'price']]
        return prices
