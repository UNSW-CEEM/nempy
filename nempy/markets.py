import numpy as np
from nempy import check, market_constraints, objective_function, solver_interface, unit_constraints, variable_ids


class Spot:
    """Class for constructing and dispatch the spot market on an interval basis.

    Initialises the spot market with general information required.

    Examples
    --------
    This is an example of the minimal set of steps for using this method.

    Import required packages.

    >>> import pandas as pd
    >>> from nempy import markets

    Define the unit information data set needed to initialise the market, in this example all units are in the same
    region.

    >>> unit_info = pd.DataFrame({
    ...     'unit': ['A', 'B'],
    ...     'region': ['NSW', 'NSW']})

    Initialise the market instance.

    >>> simple_market = markets.Spot(unit_info)

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
                     optional (as `np.int64`)
        ===========  ==============================================================================================

    dispatch_interval : int
        The length of the dispatch interval in minutes.

    Raises
    ------
        RepeatedRowError
            If there is more than one row for any unit.
        ColumnDataTypeError
            If columns are not of the require type.
        MissingColumnError
            If the column 'units' or 'regions' is missing.
        UnexpectedColumn
            There is a column that is not 'units', 'regions' or 'loss_factor'.
        ColumnValues
            If there are inf, null or negative values in the 'loss_factor' column.
    """

    @check.required_columns('unit_info', ['unit'])
    @check.column_data_types('unit_info', {'unit': str, 'region': str, 'loss_factor': np.int64})
    @check.required_columns('unit_info', ['unit', 'region', 'loss_factor'])
    @check.allowed_columns('unit_info', ['unit', 'region'])
    @check.column_values_must_be_real('unit_info', ['loss_factor'])
    @check.column_values_not_negative('unit_info', ['loss_factor'])
    def __init__(self, unit_info, dispatch_interval=5):
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
    @check.column_values_must_be_real('volume_bids', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    @check.column_values_not_negative('volume_bids', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    def set_unit_energy_volume_bids(self, volume_bids):
        """Creates the decision variables corresponding to energy bids.

        Variables are created by reserving a variable id (as `int`) for each bid. Bids with a volume of 0 MW do not
        have a variable created. The lower bound of the variables are set to zero and the upper bound to the bid
        volume, the variable type is set to continuous.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> simple_market = markets.Spot(unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

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
            1         bid volume in the 1st band, in MW (as `np.float64`)
            2         bid volume in the 2nd band, in MW (as `np.float64`)
            n         bid volume in the nth band, in MW (as `np.float64`)
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
                There is a column that is not 'units' or '1' to '10'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
        """

        # Create unit variable ids
        self.decision_variables['energy_bids'] = variable_ids.energy(volume_bids, self.next_variable_id)
        # Update the variable id counter:
        self.next_variable_id = max(self.decision_variables['energy_bids']['variable_id']) + 1

    @check.energy_bid_ids_exist
    @check.required_columns('volume_bids', ['unit'])
    @check.allowed_columns('price_bids', ['unit', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    @check.repeated_rows('price_bids', ['unit'])
    @check.column_data_types('price_bids', {'unit': str, 'else': np.float64})
    @check.column_values_must_be_real('price_bids', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    @check.bid_prices_monotonic_increasing
    def set_unit_energy_price_bids(self, price_bids):
        """Creates the objective function costs corresponding to energy bids.

        If no loss factors have been provided as part of the unit information when the model was initialised then the
        costs in the objective function are as bid. If loss factors are provided then the bid costs are referred to the
        regional reference node by dividing by the loss factor.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region. A loss factor is provided, but this is optional.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW'],
        ...     'loss_factor': [0.95, 1.1]})

        Initialise the market instance.

        >>> simple_market = markets.Spot(unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of prices for the bids. Bids for each unit need to be monotonically increasing.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_energy_price_bids(price_bids)

        The market should now have costs. Note the bid costs have been divided by the loss factors provided.

        >>> print(simple_market.objective_function_components['energy_bids'])
           variable_id unit capacity_band        cost
        0            0    A             1   52.631579
        1            1    A             2  105.263158
        2            2    A             3  105.263158
        3            3    B             1   90.909091
        4            4    B             2  118.181818
        5            5    B             3  136.363636

        Parameters
        ----------
        price_bids : pd.DataFrame
            Bids by unit, in $/MW, can contain up to n bid bands.

            ========  ======================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            1         bid price in the 1st band, in $/MW (as `np.float64`)
            2         bid price in the 2nd band, in $/MW (as `np.float64`)
            n         bid price in the nth band, in $/MW (as `np.float64`)
            ========  ======================================================

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If the volume bids have not been set yet.
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the require type.
            MissingColumnError
                If the column 'units' is missing or there are no bid bands.
            UnexpectedColumn
                There is a column that is not 'units' or '1' to '10'.
            ColumnValues
                If there are inf, -inf or null values in the bid band columns.
            BidsNotMonotonicIncreasing
                If the bids band price for all units are not monotonic increasing.
        """
        energy_objective_function = objective_function.energy(self.decision_variables['energy_bids'], price_bids)
        if 'loss_factor' in self.unit_info.columns:
            energy_objective_function = objective_function.scale_by_loss_factors(energy_objective_function,
                                                                                 self.unit_info)
        self.objective_function_components['energy_bids'] = \
            energy_objective_function.loc[:, ['variable_id', 'unit', 'capacity_band', 'cost']]

    @check.energy_bid_ids_exist
    @check.required_columns('unit_limits', ['unit', 'capacity'])
    @check.allowed_columns('unit_limits', ['unit', 'capacity'])
    @check.repeated_rows('unit_limits', ['unit'])
    @check.column_data_types('unit_limits', {'unit': str, 'else': np.float64})
    @check.column_values_must_be_real('unit_limits', ['capacity'])
    @check.column_values_not_negative('unit_limits', ['capacity'])
    def set_unit_capacity_constraints(self, unit_limits):
        """Creates constraints that limit unit output based on capacity.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> simple_market = markets.Spot(unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of unit capacities.

        >>> unit_limits = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'capacity': [60.0, 100.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_unit_capacity_constraints(unit_limits)

        The market should now have a set of constraints.

        >>> print(simple_market.constraints_rhs_and_type['unit_capacity'])
          unit  constraint_id type    rhs
        0    A              0   <=   60.0
        3    B              1   <=  100.0

        >>> print(simple_market.constraints_lhs_coefficients['unit_capacity'])
           variable_id  constraint_id  coefficient
        0            0              0            1
        1            1              0            1
        2            2              0            1
        3            3              1            1
        4            4              1            1
        5            5              1            1

        Parameters
        ----------
        unit_limits : pd.DataFrame
            Capacity by unit.

            ========  =====================================================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            capacity  The maximum output of the unit if unconstrained by ramp rate, in MW (as `np.float64`)
            ========  =====================================================================================

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If the volume bids have not been set yet.
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the require type.
            MissingColumnError
                If the column 'units' or 'capacity' is missing.
            UnexpectedColumn
                There is a column that is not 'units' or 'capacity'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
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
    @check.required_columns('unit_limits', ['unit', 'initial_output', 'ramp_up_rate'])
    @check.allowed_columns('unit_limits', ['unit', 'initial_output', 'ramp_up_rate'])
    @check.repeated_rows('unit_limits', ['unit'])
    @check.column_data_types('unit_limits', {'unit': str, 'else': np.float64})
    @check.column_values_must_be_real('unit_limits', ['initial_output', 'ramp_up_rate'])
    @check.column_values_not_negative('unit_limits', ['initial_output', 'ramp_up_rate'])
    def set_unit_ramp_up_constraints(self, unit_limits):
        """Creates constraints on unit output based on ramp up rate.

        Will constrain the unit output to be <= initial_output + (ramp_up_rate / (dispatch_interval / 60)).

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance, we set the dispatch interval to 30 min, by default it would be 5 min.

        >>> simple_market = markets.Spot(unit_info, dispatch_interval=30)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of unit ramp up rates.

        >>> unit_limits = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'initial_output': [20.0, 50.0],
        ...     'ramp_up_rate': [30.0, 100.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_unit_ramp_up_constraints(unit_limits)

        The market should now have a set of constraints.

        >>> print(simple_market.constraints_rhs_and_type['ramp_up'])
          unit  constraint_id type    rhs
        0    A              0   <=   35.0
        3    B              1   <=  100.0

        >>> print(simple_market.constraints_lhs_coefficients['ramp_up'])
           variable_id  constraint_id  coefficient
        0            0              0            1
        1            1              0            1
        2            2              0            1
        3            3              1            1
        4            4              1            1
        5            5              1            1

        Parameters
        ----------
        unit_limits : pd.DataFrame
            Capacity by unit.

            ==============  =====================================================================================
            Columns:        Description:
            unit            unique identifier of a dispatch unit (as `str`)
            initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
            ramp_up_rate    the maximum rate at which the unit can increase output, in MW/h (as `np.float64`).
            ==============  =====================================================================================

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If the volume bids have not been set yet.
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the require type.
            MissingColumnError
                If the column 'units', 'initial_output' or 'ramp_up_rate' is missing.
            UnexpectedColumn
                There is a column that is not 'units', 'initial_output' or 'ramp_up_rate'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
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
    @check.required_columns('unit_limits', ['unit', 'initial_output', 'ramp_down_rate'])
    @check.allowed_columns('unit_limits', ['unit', 'initial_output', 'ramp_down_rate'])
    @check.repeated_rows('unit_limits', ['unit'])
    @check.column_data_types('unit_limits', {'unit': str, 'else': np.float64})
    @check.column_values_must_be_real('unit_limits', ['initial_output', 'ramp_down_rate'])
    @check.column_values_not_negative('unit_limits', ['initial_output', 'ramp_down_rate'])
    def set_unit_ramp_down_constraints(self, unit_limits):
        """Creates constraints on unit output based on ramp down rate.

        Will constrain the unit output to be >= initial_output - (ramp_down_rate / (dispatch_interval / 60)).

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance, we set the dispatch interval to 30 min, by default it would be 5 min.

        >>> simple_market = markets.Spot(unit_info, dispatch_interval=30)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of unit ramp down rates, also need to provide the initial output of the units at the start of
        dispatch interval.

        >>> unit_limits = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'initial_output': [20.0, 50.0],
        ...     'ramp_down_rate': [20.0, 10.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_unit_ramp_down_constraints(unit_limits)

        The market should now have a set of constraints.

        >>> print(simple_market.constraints_rhs_and_type['ramp_down'])
           constraint_id type   rhs
        0              0   >=  10.0
        3              1   >=  45.0

        >>> print(simple_market.constraints_lhs_coefficients['ramp_down'])
           variable_id  constraint_id  coefficient
        0            0              0            1
        1            1              0            1
        2            2              0            1
        3            3              1            1
        4            4              1            1
        5            5              1            1

        Parameters
        ----------
        unit_limits : pd.DataFrame
            Capacity by unit.

            ==============  =====================================================================================
            Columns:        Description:
            unit            unique identifier of a dispatch unit (as `str`)
            initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
            ramp_up_rate    the maximum rate at which the unit can increase output, in MW/h (as `np.float64`).
            ==============  =====================================================================================

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If the volume bids have not been set yet.
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the require type.
            MissingColumnError
                If the column 'units', 'initial_output' or 'ramp_down_rate' is missing.
            UnexpectedColumn
                There is a column that is not 'units', 'initial_output' or 'ramp_down_rate'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
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
    @check.required_columns('demand', ['region', 'demand'])
    @check.allowed_columns('demand', ['region', 'demand'])
    @check.repeated_rows('demand', ['region'])
    @check.column_data_types('demand', {'region': str, 'else': np.float64})
    @check.column_values_must_be_real('demand', ['demand'])
    @check.column_values_not_negative('demand', ['demand'])
    def set_demand_constraints(self, demand):
        """Creates constraints that force supply to equal to demand.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance, we set the dispatch interval to 30 min, by default it would be 5 min.

        >>> simple_market = markets.Spot(unit_info, dispatch_interval=30)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_demand_constraints(demand)

        The market should now have a set of constraints.

        >>> print(simple_market.market_constraints_rhs_and_type['demand'])
          region  constraint_id type    rhs
        0    NSW              0    =  100.0

        >>> print(simple_market.market_constraints_lhs_coefficients['demand'])
           constraint_id  variable_id  coefficient
        0              0            0          1.0
        1              0            1          1.0
        2              0            2          1.0
        3              0            3          1.0
        4              0            4          1.0
        5              0            5          1.0

        Parameters
        ----------
        demand : pd.DataFrame
            Demand by region.

            ========  =====================================================================================
            Columns:  Description:
            region    unique identifier of a region (as `str`)
            demand    the non dispatchable demand, in MW (as `np.float64`)
            ========  =====================================================================================

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If the volume bids have not been set yet.
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the require type.
            MissingColumnError
                If the column 'region' or 'demand' is missing.
            UnexpectedColumn
                There is a column that is not 'region' or 'demand'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
        """
        # 1. Create the constraints
        lhs_coefficients, rhs_and_type = market_constraints.energy(self.decision_variables['energy_bids'],
                                                                   demand, self.unit_info, self.next_constraint_id)
        # 2. Save constraint details
        self.market_constraints_lhs_coefficients['demand'] = lhs_coefficients
        self.market_constraints_rhs_and_type['demand'] = rhs_and_type
        # 3. Update the constraint id
        self.next_constraint_id = max(lhs_coefficients['constraint_id']) + 1

    @check.pre_dispatch
    def dispatch(self):
        """Combines the elements of the linear program and solves to find optimal dispatch.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance, we set the dispatch interval to 30 min, by default it would be 5 min.

        >>> simple_market = markets.Spot(unit_info, dispatch_interval=30)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_energy_price_bids(price_bids)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_demand_constraints(demand)

        Call the dispatch method.

        >>> simple_market.dispatch()

        Now the market dispatch can be retrieved.

        >>> print(simple_market.get_energy_dispatch())
          unit  dispatch
        0    A      45.0
        1    B      55.0

        And the market prices can be retrieved.

        >>> print(simple_market.get_energy_prices())
          region  price
        0    NSW  130.0

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If a model build process is incomplete, i.e. there are energy bids but not energy demand set.
        """
        decision_variables, market_constraints_rhs_and_type = solver_interface.dispatch(
            self.decision_variables, self.constraints_lhs_coefficients, self.constraints_rhs_and_type,
            self.market_constraints_lhs_coefficients, self.market_constraints_rhs_and_type,
            self.objective_function_components)
        self.market_constraints_rhs_and_type = market_constraints_rhs_and_type
        self.decision_variables = decision_variables

    def get_energy_dispatch(self):
        """Retrieves the energy dispatch for each unit.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance, we set the dispatch interval to 30 min, by default it would be 5 min.

        >>> simple_market = markets.Spot(unit_info, dispatch_interval=30)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_energy_price_bids(price_bids)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_demand_constraints(demand)

        Call the dispatch method.

        >>> simple_market.dispatch()

        Now the market dispatch can be retrieved.

        >>> print(simple_market.get_energy_dispatch())
          unit  dispatch
        0    A      45.0
        1    B      55.0

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If a model build process is incomplete, i.e. there are energy bids but not energy demand set.
        """
        dispatch = self.decision_variables['energy_bids'].loc[:, ['unit', 'value']]
        dispatch.columns = ['unit', 'dispatch']
        return dispatch.groupby('unit', as_index=False).sum()

    def get_energy_prices(self):
        """Retrieves the energy price in each market region.

        Energy prices are the shadow prices of the demand constraint in each market region.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance, we set the dispatch interval to 30 min, by default it would be 5 min.

        >>> simple_market = markets.Spot(unit_info, dispatch_interval=30)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_energy_price_bids(price_bids)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_demand_constraints(demand)

        Call the dispatch method.

        >>> simple_market.dispatch()

        Now the market prices can be retrieved.

        >>> print(simple_market.get_energy_prices())
          region  price
        0    NSW  130.0

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If a model build process is incomplete, i.e. there are energy bids but not energy demand set.
        """
        prices = self.market_constraints_rhs_and_type['demand'].loc[:, ['region', 'price']]
        return prices
