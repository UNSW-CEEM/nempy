import numpy as np
import pandas as pd
from nempy import check, market_constraints, objective_function, solver_interface, unit_constraints, variable_ids, \
    create_lhs, interconnectors



class Spot:
    """Class for constructing and dispatch the spot market on an interval basis."""

    def __init__(self, dispatch_interval=5):
        self.dispatch_interval = dispatch_interval
        self.unit_info = None
        self.decision_variables = {}
        self.lhs_coefficients = pd.DataFrame()
        self.constraints_rhs_and_type = {}
        self.constraints_dynamic_rhs_and_type = {}
        self.constraints_rhs_and_type_no_lhs_yet = {}
        self.market_constraints_rhs_and_type_no_lhs_yet = {}
        self.objective_function_components = {}
        self.next_variable_id = 0
        self.next_constraint_id = 0
        self.check = True

    @check.required_columns('unit_info', ['unit'])
    @check.column_data_types('unit_info', {'unit': str, 'region': str, 'loss_factor': np.float64})
    @check.required_columns('unit_info', ['unit', 'region'])
    @check.allowed_columns('unit_info', ['unit', 'region', 'loss_factor'])
    @check.column_values_must_be_real('unit_info', ['loss_factor'])
    @check.column_values_not_negative('unit_info', ['loss_factor'])
    def set_unit_info(self, unit_info):
        """Add general information required.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
                If there are inf, null or negative values in the 'loss_factor' column."""
        self.unit_info = unit_info

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

        Also clears any preexisting constraints sets or objective functions that depend on the energy bid decision
        variables.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
           variable_id unit capacity_band  ...  region  service coefficient
        0            0    A             1  ...     NSW   energy         1.0
        1            1    A             2  ...     NSW   energy         1.0
        2            2    A             3  ...     NSW   energy         1.0
        3            3    B             1  ...     NSW   energy         1.0
        4            4    B             2  ...     NSW   energy         1.0
        5            5    B             3  ...     NSW   energy         1.0

        Parameters
        ----------
        volume_bids : pd.DataFrame
            Bids by unit, in MW, can contain up to 10 bid bands, these should be labeled '1' to '10'.

            ========  ======================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            1         bid volume in the 1st band, in MW (as `np.float64`)
            2         bid volume in the 2nd band, in MW (as `np.float64`)
              :
            10         bid volume in the nth band, in MW (as `np.float64`)
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
        self.decision_variables['energy_bids'] = variable_ids.energy(volume_bids, self.unit_info, self.next_variable_id)
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

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
           variable_id unit capacity_band   cost
        0            0    A             1   50.0
        1            1    A             2  100.0
        2            2    A             3  100.0
        3            3    B             1  100.0
        4            4    B             2  130.0
        5            5    B             3  150.0

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

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
          unit  constraint_id type    rhs  coefficient service
        0    A              0   <=   60.0          1.0  energy
        1    B              1   <=  100.0          1.0  energy

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
        rhs_and_type = unit_constraints.capacity(unit_limits, self.next_constraint_id)
        # 2. Save constraint details.
        self.constraints_rhs_and_type_no_lhs_yet['unit_capacity'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

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

        Initialise the market instance, we set the dispatch interval to 30 min, by default it would be 5 min.

        >>> simple_market = markets.Spot(dispatch_interval=30)

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
          unit  constraint_id type    rhs  coefficient service
        0    A              0   <=   35.0          1.0  energy
        1    B              1   <=  100.0          1.0  energy

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
        rhs_and_type = unit_constraints.ramp_up(unit_limits, self.next_constraint_id, self.dispatch_interval)
        # 2. Save constraint details.
        self.constraints_rhs_and_type_no_lhs_yet['ramp_up'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

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

        Initialise the market instance, we set the dispatch interval to 30 min, by default it would be 5 min.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
          unit  constraint_id type        rhs  coefficient service
        0    A              0   >=  18.333333          1.0  energy
        1    B              1   >=  49.166667          1.0  energy

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
        rhs_and_type = unit_constraints.ramp_down(unit_limits, self.next_constraint_id, self.dispatch_interval)
        # 2. Save constraint details.
        self.constraints_rhs_and_type_no_lhs_yet['ramp_down'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

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

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
          region  constraint_id type    rhs  coefficient service
        0    NSW              0    =  100.0          1.0  energy

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
        rhs_and_type = market_constraints.energy(demand,  self.next_constraint_id)
        # 2. Save constraint details
        self.market_constraints_rhs_and_type_no_lhs_yet['demand'] = rhs_and_type
        # 3. Update the constraint id
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

    @check.required_columns('interconnector_directions_and_limits',
                            ['interconnector', 'to_region', 'from_region', 'max', 'min'])
    @check.allowed_columns('interconnector_directions_and_limits',
                           ['interconnector', 'to_region', 'from_region', 'max', 'min'])
    @check.repeated_rows('interconnector_directions_and_limits', ['interconnector'])
    @check.column_data_types('interconnector_directions_and_limits',
                             {'interconnector': str, 'to_region': str, 'from_region' : str, 'max' : np.float64,
                              'min': np.float64})
    @check.column_values_must_be_real('interconnector_directions_and_limits', ['min', 'max'])
    def set_interconnectors(self, interconnector_directions_and_limits):
        """Creates a lossless link between specified regions.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A'],
        ...     'region': ['NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

        Define a set of bids, in this example we have just one unit that can provide 100 MW in NSW.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [100.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [80.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_energy_price_bids(price_bids)

        Define a demand level in each region, no power is required in NSW and 90.0 MW is required in VIC.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW', 'VIC'],
        ...     'demand': [0.0, 90.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_demand_constraints(demand)

        Define a an interconnector between NSW and VIC so generator can A can be used to meet demand in VIC.

        >>> interconnector = pd.DataFrame({
        ...     'interconnector': ['inter_one'],
        ...     'to_region': ['VIC'],
        ...     'from_region': ['NSW'],
        ...     'max': [100.0],
        ...     'min': [-100.0]})

        Create the interconnector.

        >>> simple_market.set_interconnectors(interconnector)

        Call the dispatch method.

        >>> simple_market.dispatch()

        Now the market dispatch can be retrieved.

        >>> print(simple_market.get_energy_dispatch())
          unit  dispatch
        0    A      90.0

        And the interconnector flows can be retrieved.

        >>> print(simple_market.get_interconnector_flows())
          interconnector  flow
        0      inter_one  90.0

        Parameters
        ----------
        interconnector_directions_and_limits : pd.DataFrame
            Interconnector definition.

            ==============  =====================================================================================
            Columns:        Description:
            interconnector  unique identifier of a interconnector (as `str`)
            to_region       the region that receives power when flow is in the positive direction (as `str`)
            from_region     the region that power is drawn from when flow is in the positive direction (as `str`)
            max             the maximum power flow in the positive direction, in MW (as `np.float64`)
            min             the maximum power flow in the negative direction, in MW (as `np.float64`)
            ==============  ====================================================================================

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any interconnector.
            ColumnDataTypeError
                If columns are not of the require type.
            MissingColumnError
                If any columns are missing.
            UnexpectedColumn
                If there are any additional columns in the input DataFrame.
            ColumnValues
                If there are inf, null values in the max and min columns.
        """
        # Create unit variable ids
        self.decision_variables['interconnectors'] = interconnectors.create(interconnector_directions_and_limits,
                                                                            self.next_variable_id)
        # Update the variable id counter:
        self.next_variable_id = max(self.decision_variables['interconnectors']['variable_id']) + 1

    @check.required_columns('interpolation_break_point', ['interconnector', 'break_point'], arg=1)
    @check.allowed_columns('interpolation_break_point', ['interconnector', 'break_point'], arg=1)
    @check.repeated_rows('interpolation_break_point', ['interconnector', 'break_point'], arg=1)
    @check.column_data_types('interpolation_break_point', {'interconnector': str, 'break_point': np.float64}, arg=1)
    @check.column_values_must_be_real('interpolation_break_point', ['break_point'], arg=1)
    @check.required_columns('loss_functions', ['interconnector', 'from_region_loss_share', 'loss_function'], arg=2)
    @check.allowed_columns('loss_functions', ['interconnector', 'from_region_loss_share', 'loss_function'], arg=2)
    @check.repeated_rows('loss_functions', ['interconnector'], arg=2)
    @check.column_data_types('loss_functions', {'interconnector': str, 'from_region_loss_share': np.float64,
                                                'loss_function': 'callable'}, arg=2)
    @check.column_values_must_be_real('loss_functions', ['break_point'], arg=2)
    def set_interconnector_losses(self, interpolation_break_points, loss_functions):
        loss_variables, weight_variables, lhs, weights_sum_rhs, dynamic_rhs = \
            interconnectors.add_losses(interpolation_break_points, self.decision_variables['interconnectors'],
                                       loss_functions, self.next_variable_id, self.next_constraint_id)
        self.decision_variables['interconnector_losses'] = loss_variables
        self.decision_variables['interpolation_weights'] = weight_variables
        self.lhs_coefficients = pd.concat([self.lhs_coefficients, lhs])
        self.constraints_rhs_and_type['interpolation_weights'] = weights_sum_rhs
        self.constraints_dynamic_rhs_and_type['link_loss_to_flow'] = dynamic_rhs
        self.next_variable_id = pd.concat([loss_variables, weight_variables])['variable_id'].max() + 1
        self.next_constraint_id = pd.concat([weights_sum_rhs, dynamic_rhs])['constraint_id'].max() + 1


    @check.pre_dispatch
    def dispatch(self):
        """Combines the elements of the linear program and solves to find optimal dispatch.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
        constraints_lhs = pd.concat([self.lhs_coefficients,
            create_lhs.create(self.market_constraints_rhs_and_type_no_lhs_yet, self.decision_variables,
                              ['region', 'service'])])

        if len(self.constraints_rhs_and_type_no_lhs_yet) > 0:
            unit_constraints_lhs = create_lhs.create(self.constraints_rhs_and_type_no_lhs_yet, self.decision_variables,
                                                     ['unit', 'service'])
            constraints_lhs = pd.concat([constraints_lhs, unit_constraints_lhs])

        constraints_rhs_and_type = pd.concat(list(self.constraints_rhs_and_type.values()) +
                                             list(self.constraints_rhs_and_type_no_lhs_yet.values()))

        decision_variables, market_constraints_rhs_and_type = solver_interface.dispatch(
            self.decision_variables, constraints_lhs, constraints_rhs_and_type,
            self.market_constraints_rhs_and_type_no_lhs_yet, self.constraints_dynamic_rhs_and_type,
            self.objective_function_components)
        self.market_constraints_rhs_and_type_no_lhs_yet = market_constraints_rhs_and_type
        self.decision_variables = decision_variables

    def get_energy_dispatch(self):
        """Retrieves the energy dispatch for each unit.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
        pd.DataFrame

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

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

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
        pd.DateFrame

        Raises
        ------
            ModelBuildError
                If a model build process is incomplete, i.e. there are energy bids but not energy demand set.
        """
        prices = self.market_constraints_rhs_and_type_no_lhs_yet['demand'].loc[:, ['region', 'price']]
        return prices

    def get_interconnector_flows(self):
        """Retrieves the  flows for each interconnector.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A'],
        ...     'region': ['NSW']})

        Add unit information

        >>> simple_market.set_unit_info(unit_info)

        Define a set of bids, in this example we have just one unit that can provide 100 MW in NSW.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [100.0]})

        Create energy unit bid decision variables.

        >>> simple_market.set_unit_energy_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [80.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_energy_price_bids(price_bids)

        Define a demand level in each region, no power is required in NSW and 90.0 MW is required in VIC.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW', 'VIC'],
        ...     'demand': [0.0, 90.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_demand_constraints(demand)

        Define a an interconnector between NSW and VIC so generator can A can be used to meet demand in VIC.

        >>> interconnector = pd.DataFrame({
        ...     'interconnector': ['inter_one'],
        ...     'to_region': ['VIC'],
        ...     'from_region': ['NSW'],
        ...     'max': [100.0],
        ...     'min': [-100.0]})

        Create the interconnector.

        >>> simple_market.set_interconnectors(interconnector)

        Call the dispatch method.

        >>> simple_market.dispatch()

        Now the market dispatch can be retrieved.

        >>> print(simple_market.get_energy_dispatch())
          unit  dispatch
        0    A      90.0

        And the interconnector flows can be retrieved.

        >>> print(simple_market.get_interconnector_flows())
          interconnector  flow
        0      inter_one  90.0

        Returns
        -------
        pd.DataFrame

        Raises
        ------
            ModelBuildError
                If a model build process is incomplete, i.e. there are energy bids but not energy demand set.
        """
        dispatch = self.decision_variables['interconnectors'].loc[:, ['interconnector', 'value']]
        dispatch.columns = ['interconnector', 'flow']
        return dispatch.drop_duplicates('interconnector').reset_index(drop=True)
