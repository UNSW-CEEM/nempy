import numpy as np
import pandas as pd
from nempy import check, market_constraints, objective_function, solver_interface, unit_constraints, variable_ids, \
    create_lhs, interconnectors as inter, fcas_constraints


class Spot:
    """Class for constructing and dispatch the spot market on an interval basis."""

    def __init__(self, dispatch_interval=5):
        self.dispatch_interval = dispatch_interval
        self.unit_info = None
        self.decision_variables = {}
        self.variable_to_constraint_map = {'regional': {}, 'unit_level': {}}
        self.constraint_to_variable_map = {'regional': {}, 'unit_level': {}}
        self.lhs_coefficients = pd.DataFrame()
        self.constraints_rhs_and_type = {}
        self.constraints_dynamic_rhs_and_type = {}
        self.market_constraints_rhs_and_type = {}
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
    @check.allowed_columns('volume_bids', ['unit', 'service', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    @check.repeated_rows('volume_bids', ['unit', 'service'])
    @check.column_data_types('volume_bids', {'unit': str, 'service': str, 'else': np.float64})
    @check.column_values_must_be_real('volume_bids', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    @check.column_values_not_negative('volume_bids', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    def set_unit_volume_bids(self, volume_bids):
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

        >>> print(simple_market.decision_variables['bids'])
          unit capacity_band service  variable_id  lower_bound  upper_bound        type
        0    A             1  energy            0          0.0         20.0  continuous
        1    A             2  energy            1          0.0         20.0  continuous
        2    A             3  energy            2          0.0          5.0  continuous
        3    B             1  energy            3          0.0         50.0  continuous
        4    B             2  energy            4          0.0         30.0  continuous
        5    B             3  energy            5          0.0         10.0  continuous

        A mapping of these variables to constraints acting on that unit and service should also exist.

        >>> print(simple_market.variable_to_constraint_map['unit_level']['bids'])
           variable_id unit service  coefficient
        0            0    A  energy          1.0
        1            1    A  energy          1.0
        2            2    A  energy          1.0
        3            3    B  energy          1.0
        4            4    B  energy          1.0
        5            5    B  energy          1.0

        A mapping of these variables to constraints acting on the units region and service should also exist.

        >>> print(simple_market.variable_to_constraint_map['regional']['bids'])
           variable_id region service  coefficient
        0            0    NSW  energy          1.0
        1            1    NSW  energy          1.0
        2            2    NSW  energy          1.0
        3            3    NSW  energy          1.0
        4            4    NSW  energy          1.0
        5            5    NSW  energy          1.0

        Parameters
        ----------
        volume_bids : pd.DataFrame
            Bids by unit, in MW, can contain up to 10 bid bands, these should be labeled '1' to '10'.

            ========  ===============================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            service   the service being provided, optional, if missing energy assumed
                      (as `str`)
            1         bid volume in the 1st band, in MW (as `np.float64`)
            2         bid volume in the 2nd band, in MW (as `np.float64`)
              :
            10         bid volume in the nth band, in MW (as `np.float64`)
            ========  ================================================================

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

        # Create unit variable ids and their mapping into constraints.
        self.decision_variables['bids'], variable_to_constraint_map = \
            variable_ids.bids(volume_bids, self.unit_info, self.next_variable_id)

        # Split constraint mapping up on a regional and unit level basis.
        self.variable_to_constraint_map['regional']['bids'] = \
            variable_to_constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]
        self.variable_to_constraint_map['unit_level']['bids'] = \
            variable_to_constraint_map.loc[:, ['variable_id', 'unit', 'service', 'coefficient']]

        # Update the variable id counter:
        self.next_variable_id = max(self.decision_variables['bids']['variable_id']) + 1

    @check.energy_bid_ids_exist
    @check.required_columns('price_bids', ['unit'])
    @check.allowed_columns('price_bids', ['unit', 'service', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    @check.repeated_rows('price_bids', ['unit', 'service'])
    @check.column_data_types('price_bids', {'unit': str, 'service': str, 'else': np.float64})
    @check.column_values_must_be_real('price_bids', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
    @check.bid_prices_monotonic_increasing
    def set_unit_price_bids(self, price_bids):
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

        The market should now have costs.

        >>> print(simple_market.objective_function_components['bids'])
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
            Bids by unit, in $/MW, can contain up to 10 bid bands.

            ========  ======================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            service   the service being provided, optional, if missing energy assumed
                      (as `str`)
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
        energy_objective_function = objective_function.bids(self.decision_variables['bids'], price_bids)
        if 'loss_factor' in self.unit_info.columns:
            energy_objective_function = objective_function.scale_by_loss_factors(energy_objective_function,
                                                                                 self.unit_info)
        self.objective_function_components['bids'] = \
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
          unit  constraint_id type    rhs
        0    A              0   <=   60.0
        1    B              1   <=  100.0

        ... and a mapping of those constraints to the variable types on the lhs.

        >>> print(simple_market.constraint_to_variable_map['unit_level']['unit_capacity'])
           constraint_id unit service  coefficient
        0              0    A  energy          1.0
        1              1    B  energy          1.0


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
        rhs_and_type, variable_map = unit_constraints.capacity(unit_limits, self.next_constraint_id)
        # 2. Save constraint details.
        self.constraints_rhs_and_type['unit_capacity'] = rhs_and_type
        self.constraint_to_variable_map['unit_level']['unit_capacity'] = variable_map
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
          unit  constraint_id type    rhs
        0    A              0   <=   35.0
        1    B              1   <=  100.0

        ... and a mapping of those constraints to variable type for the lhs.

        >>> print(simple_market.constraint_to_variable_map['unit_level']['ramp_up'])
           constraint_id unit service  coefficient
        0              0    A  energy          1.0
        1              1    B  energy          1.0

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
        rhs_and_type, variable_map = unit_constraints.ramp_up(unit_limits, self.next_constraint_id,
                                                              self.dispatch_interval)
        # 2. Save constraint details.
        self.constraints_rhs_and_type['ramp_up'] = rhs_and_type
        self.constraint_to_variable_map['unit_level']['ramp_up'] = variable_map
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

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
          unit  constraint_id type        rhs
        0    A              0   >=  18.333333
        1    B              1   >=  49.166667

        ... and a mapping of those constraints to variable type for the lhs.

        >>> print(simple_market.constraint_to_variable_map['unit_level']['ramp_down'])
           constraint_id unit service  coefficient
        0              0    A  energy          1.0
        1              1    B  energy          1.0

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
        rhs_and_type, variable_map = unit_constraints.ramp_down(unit_limits, self.next_constraint_id,
                                                                self.dispatch_interval)
        # 2. Save constraint details.
        self.constraints_rhs_and_type['ramp_down'] = rhs_and_type
        self.constraint_to_variable_map['unit_level']['ramp_down'] = variable_map
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

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

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create constraints.

        >>> simple_market.set_demand_constraints(demand)

        The market should now have a set of constraints.

        >>> print(simple_market.market_constraints_rhs_and_type['demand'])
          region  constraint_id type    rhs
        0    NSW              0    =  100.0

        ... and a mapping of those constraints to variable type for the lhs.

        >>> print(simple_market.constraint_to_variable_map['regional']['demand'])
           constraint_id region service  coefficient
        0              0    NSW  energy          1.0

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
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the column 'region' or 'demand' is missing.
            UnexpectedColumn
                There is a column that is not 'region' or 'demand'.
            ColumnValues
                If there are inf, null or negative values in the volume column.
        """
        # 1. Create the constraints
        rhs_and_type, variable_map = market_constraints.energy(demand, self.next_constraint_id)
        # 2. Save constraint details
        self.market_constraints_rhs_and_type['demand'] = rhs_and_type
        self.constraint_to_variable_map['regional']['demand'] = variable_map
        # 3. Update the constraint id
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

    @check.required_columns('fcas_requirements', ['set', 'service', 'region', 'volume'])
    @check.allowed_columns('fcas_requirements', ['set', 'service', 'region', 'volume'])
    @check.repeated_rows('fcas_requirements', ['set', 'service', 'region'])
    @check.column_data_types('fcas_requirements', {'set': str, 'service': str, 'region': str, 'else': np.float64})
    @check.column_values_must_be_real('fcas_requirements', ['volume'])
    @check.column_values_not_negative('fcas_requirements', ['volume'])
    def set_fcas_requirements_constraints(self, fcas_requirements):
        """Creates constraints that force FCAS supply to equal requirements.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define a regulation raise FCAS requirement that apply to all mainland states.

        >>> fcas_requirements = pd.DataFrame({
        ...     'set': ['raise_reg_main', 'raise_reg_main', 'raise_reg_main', 'raise_reg_main'],
        ...     'service': ['raise_reg', 'raise_reg', 'raise_reg', 'raise_reg'],
        ...     'region': ['QLD', 'NSW', 'VIC', 'SA'],
        ...     'volume': [100.0, 100.0, 100.0, 100.0]})

        Create constraints.

        >>> simple_market.set_fcas_requirements_constraints(fcas_requirements)

        The market should now have a set of constraints.

        >>> print(simple_market.market_constraints_rhs_and_type['fcas'])
                      set  constraint_id type    rhs
        0  raise_reg_main              0    =  100.0

        ... and a mapping of those constraints to variable type for the lhs.

        >>> print(simple_market.constraint_to_variable_map['regional']['fcas'])
           constraint_id    service region  coefficient
        0              0  raise_reg    QLD          1.0
        1              0  raise_reg    NSW          1.0
        2              0  raise_reg    VIC          1.0
        3              0  raise_reg     SA          1.0

        Parameters
        ----------
        fcas_requirements : pd.DataFrame
            requirement by set and the regions and service the requirement applies to.

            ========  ===================================================================
            Columns:  Description:
            set       unique identifier of the requirement set (as `str`)
            service   the service or services the requirement set applies to (as `str`)
            region    unique identifier of a region (as `str`)
            volume    the amount of service required, in MW (as `np.float64`)
            ========  ===================================================================

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the column 'set', 'service', 'region', or 'demand' is missing.
            UnexpectedColumn
                There is a column that is not 'set', 'service', 'region', or 'demand'.
            ColumnValues
                If there are inf, null or negative values in the volume column.
        """
        # 1. Create the constraints
        rhs_and_type, variable_map = market_constraints.fcas(fcas_requirements, self.next_constraint_id)
        # 2. Save constraint details
        self.market_constraints_rhs_and_type['fcas'] = rhs_and_type
        self.constraint_to_variable_map['regional']['fcas'] = variable_map
        # 3. Update the constraint id
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

    @check.required_columns('fcas_max_availability', ['unit', 'service', 'max_availability'], arg=1)
    @check.allowed_columns('fcas_max_availability', ['unit', 'service', 'max_availability'], arg=1)
    @check.repeated_rows('fcas_max_availability', ['unit', 'service'], arg=1)
    @check.column_data_types('fcas_max_availability', {'unit': str, 'service': str, 'else': np.float64}, arg=1)
    @check.column_values_must_be_real('fcas_max_availability', ['max_availability'], arg=1)
    @check.column_values_not_negative('fcas_max_availability', ['max_availability'], arg=1)
    def set_fcas_max_availability(self, fcas_max_availability):
        """Creates constraints to ensure fcas dispatch is limited to the availability specified in the FCAS trapezium.

        The constraints are described in the
        :download:`FCAS MODEL IN NEMDE documentation section 2  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        Examples
        --------

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot(dispatch_interval=60)

        Define the FCAS max_availability.

        >>> fcas_max_availability = pd.DataFrame({
        ... 'unit': ['A'],
        ... 'service': ['raise_6s'],
        ... 'max_availability': [60.0]})

        Set the joint availability constraints.

        >>> simple_market.set_fcas_max_availability(fcas_max_availability)

        TNow the market should have the constraints and their mapping to decision varibales.

        >>> print(simple_market.constraints_rhs_and_type['fcas_max_availability'])
          unit   service  constraint_id type   rhs
        0    A  raise_6s              0   <=  60.0

        >>> print(simple_market.constraint_to_variable_map['unit_level']['fcas_max_availability'])
           constraint_id unit   service  coefficient
        0              0    A  raise_6s          1.0

        Parameters
        ----------
        fcas_max_availability : pd.DataFrame
            The FCAS max_availability for the services being offered.

            ================   ======================================================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit (as `str`)
            service            the contingency service being offered (as `str`)
            max_availability   the maximum volume of the contingency service in MW (as `np.float64`)
            ================   ======================================================================

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit and service combination in contingency_trapeziums.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the columns 'unit', 'service' or 'max_availability' is missing from fcas_max_availability.
            UnexpectedColumn
                If there are columns other than 'unit', 'service' or 'max_availability' in fcas_max_availability.
            ColumnValues
                If there are inf, null or negative values in the columns of type `np.float64`.
        """

        rhs_and_type, variable_map = unit_constraints.fcas_max_availability(fcas_max_availability,
                                                                            self.next_constraint_id)

        self.constraints_rhs_and_type['fcas_max_availability'] = rhs_and_type
        self.constraint_to_variable_map['unit_level']['fcas_max_availability'] = variable_map
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

    @check.required_columns('regulation_units', ['unit', 'service'], arg=1)
    @check.allowed_columns('regulation_units', ['unit', 'service'], arg=1)
    @check.repeated_rows('regulation_units', ['unit', 'service'], arg=1)
    @check.column_data_types('regulation_units', {'unit': str, 'service': str}, arg=1)
    @check.required_columns('unit_limits', ['unit', 'initial_output', 'ramp_up_rate', 'ramp_down_rate'], arg=2)
    @check.allowed_columns('unit_limits', ['unit', 'initial_output', 'ramp_up_rate', 'ramp_down_rate'], arg=2)
    @check.repeated_rows('unit_limits', ['unit'], arg=2)
    @check.column_data_types('unit_limits', {'unit': str, 'else': np.float64}, arg=2)
    @check.column_values_must_be_real('unit_limits', ['initial_output', 'ramp_up_rate', 'ramp_down_rate'], arg=2)
    @check.column_values_not_negative('unit_limits', ['initial_output', 'ramp_up_rate', 'ramp_down_rate'], arg=2)
    def set_joint_ramping_constraints(self, regulation_units, unit_limits):
        """Create constraints that ensure the provision of energy and fcas are within unit ramping capabilities.

        The constraints are described in the
        :download:`FCAS MODEL IN NEMDE documentation section 6.1  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        On a unit basis they take the form of:

            Energy dispatch + Regulation raise target <= initial output + ramp up rate / (dispatch interval / 60)

        and

            Energy dispatch + Regulation lower target <= initial output - ramp down rate / (dispatch interval / 60)

        Examples
        --------

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot(dispatch_interval=60)

        Define the set of units providing regulation services.

        >>> regulation_units = pd.DataFrame({
        ...   'unit': ['A', 'B', 'B'],
        ...   'service': ['raise_reg', 'lower_reg', 'raise_reg']})

        Define unit initial outputs and ramping capabilities.

        >>> unit_limits = pd.DataFrame({
        ...   'unit': ['A', 'B'],
        ...   'initial_output': [100.0, 80.0],
        ...   'ramp_up_rate': [20.0, 10.0],
        ...   'ramp_down_rate': [15.0, 25.0]})

        Create the joint ramping constraints.

        >>> simple_market.set_joint_ramping_constraints(regulation_units, unit_limits)

        Now the market should have the constraints and their mapping to decision varibales.

        >>> print(simple_market.constraints_rhs_and_type['joint_ramping'])
          unit  constraint_id type    rhs
        0    A              0   <=  120.0
        1    B              1   >=   55.0
        2    B              2   <=   90.0

        >>> print(simple_market.constraint_to_variable_map['unit_level']['joint_ramping'])
           constraint_id unit    service  coefficient
        0              0    A  raise_reg          1.0
        1              1    B  lower_reg          1.0
        2              2    B  raise_reg          1.0
        0              0    A     energy          1.0
        1              1    B     energy          1.0
        2              2    B     energy          1.0

        Parameters
        ----------
        regulation_units : pd.DataFrame
            The units with bids submitted to provide regulation FCAS

            ========  =======================================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            service   the regulation service being bid for raise_reg or lower_reg  (as `str`)
            ========  =======================================================================

        unit_limits : pd.DataFrame
            The initial output and ramp rates of units
            ==============  =====================================================================================
            Columns:        Description:
            unit            unique identifier of a dispatch unit (as `str`)
            initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
            ramp_up_rate    the maximum rate at which the unit can increase output, in MW/h (as `np.float64`)
            ramp_down_rate  the maximum rate at which the unit can decrease output, in MW/h (as `np.float64`)
            ==============  =====================================================================================

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit and service combination in regulation_units, or if there is
                more than one row for any unit in unit_limits.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the columns 'unit' or 'service' are missing from regulations_units, or if the columns 'unit',
                'initial_output', 'ramp_up_rate' or 'ramp_down_rate' are missing from unit_limits.
            UnexpectedColumn
                If there are columns other than 'unit' or 'service' in regulations_units, or if there are columns other
                than 'unit', 'initial_output', 'ramp_up_rate' or 'ramp_down_rate' in unit_limits.
            ColumnValues
                If there are inf, null or negative values in the columns of type `np.float64`.
        """

        rhs_and_type, variable_map = fcas_constraints.joint_ramping_constraints(regulation_units, unit_limits,
                                                                                self.dispatch_interval,
                                                                                self.next_constraint_id)
        self.constraints_rhs_and_type['joint_ramping'] = rhs_and_type
        self.constraint_to_variable_map['unit_level']['joint_ramping'] = variable_map
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

    @check.required_columns('contingency_trapeziums', ['unit', 'service', 'max_availability', 'enablement_min',
                                                       'low_break_point', 'high_break_point', 'enablement_max'], arg=1)
    @check.allowed_columns('contingency_trapeziums', ['unit', 'service', 'max_availability', 'enablement_min',
                                                      'low_break_point', 'high_break_point', 'enablement_max'], arg=1)
    @check.repeated_rows('contingency_trapeziums', ['unit', 'service'], arg=1)
    @check.column_data_types('contingency_trapeziums', {'unit': str, 'service': str, 'else': np.float64}, arg=1)
    @check.column_values_must_be_real('contingency_trapeziums', ['max_availability', 'enablement_min',
                                       'low_break_point', 'high_break_point', 'enablement_max'], arg=1)
    @check.column_values_not_negative('contingency_trapeziums', ['max_availability', 'enablement_min',
                                       'low_break_point', 'high_break_point', 'enablement_max'], arg=1)
    def set_joint_capacity_constraints(self, contingency_trapeziums):
        """Creates constraints to ensure there is adequate capacity for contingency, regulation and energy dispatch.

        Create two constraints for each contingency services, one ensures operation on upper slope of the fcas
        contingency trapezium is consistent with regulation raise and energy dispatch, the second ensures operation on
        upper slope of the fcas contingency trapezium is consistent with regulation lower and energy dispatch.

        The constraints are described in the
        :download:`FCAS MODEL IN NEMDE documentation section 6.2  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        Examples
        --------

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot(dispatch_interval=60)

        Define the FCAS contingency trapeziums.

        >>> contingency_trapeziums = pd.DataFrame({
        ... 'unit': ['A'],
        ... 'service': ['raise_6s'],
        ... 'max_availability': [60.0],
        ... 'enablement_min': [20.0],
        ... 'low_break_point': [40.0],
        ... 'high_break_point': [60.0],
        ... 'enablement_max': [80.0]})

        Set the joint capacity constraints.

        >>> simple_market.set_joint_capacity_constraints(contingency_trapeziums)

        TNow the market should have the constraints and their mapping to decision varibales.

        >>> print(simple_market.constraints_rhs_and_type['joint_capacity'])
          unit   service  constraint_id type   rhs
        0    A  raise_6s              0   <=  80.0
        0    A  raise_6s              1   >=  20.0

        >>> print(simple_market.constraint_to_variable_map['unit_level']['joint_capacity'])
           constraint_id unit    service  coefficient
        0              0    A     energy     1.000000
        0              0    A   raise_6s     0.333333
        0              0    A  raise_reg     1.000000
        0              1    A     energy     1.000000
        0              1    A   raise_6s    -0.333333
        0              1    A  lower_reg    -1.000000

        Parameters
        ----------
        contingency_trapeziums : pd.DataFrame
            The FCAS trapeziums for the contingency services being offered.

            ================   ======================================================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit (as `str`)
            service            the contingency service being offered (as `str`)
            max_availability   the maximum volume of the contingency service in MW (as `np.float64`)
            enablement_min     the energy dispatch level at which the unit can begin to provide the
                               contingency service, in MW (as `np.float64`)
            low_break_point    the energy dispatch level at which the unit can provide the full
                               contingency service offered, in MW (as `np.float64`)
            high_break_point   the energy dispatch level at which the unit can no longer provide the
                               full contingency service offered, in MW (as `np.float64`)
            enablement_max     the energy dispatch level at which the unit can no longer begin
                               the contingency service, in MW (as `np.float64`)
            ================   ======================================================================

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit and service combination in contingency_trapeziums.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the columns 'unit', 'service', 'max_availability', 'enablement_min', 'low_break_point',
                'high_break_point' or 'enablement_max' from contingency_trapeziums.
            UnexpectedColumn
                If there are columns other than 'unit', 'service', 'max_availability', 'enablement_min',
                'low_break_point', 'high_break_point' or 'enablement_max' in contingency_trapeziums.
            ColumnValues
                If there are inf, null or negative values in the columns of type `np.float64`.
        """

        rhs_and_type, variable_map = fcas_constraints.joint_capacity_constraints(contingency_trapeziums,
                                                                                 self.next_constraint_id)
        self.constraints_rhs_and_type['joint_capacity'] = rhs_and_type
        self.constraint_to_variable_map['unit_level']['joint_capacity'] = variable_map
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

    @check.required_columns('regulation_trapeziums', ['unit', 'service', 'max_availability', 'enablement_min',
                                                       'low_break_point', 'high_break_point', 'enablement_max'], arg=1)
    @check.allowed_columns('regulation_trapeziums', ['unit', 'service', 'max_availability', 'enablement_min',
                                                      'low_break_point', 'high_break_point', 'enablement_max'], arg=1)
    @check.repeated_rows('regulation_trapeziums', ['unit', 'service'], arg=1)
    @check.column_data_types('regulation_trapeziums', {'unit': str, 'service': str, 'else': np.float64}, arg=1)
    @check.column_values_must_be_real('regulation_trapeziums', ['max_availability', 'enablement_min',
                                       'low_break_point', 'high_break_point', 'enablement_max'], arg=1)
    @check.column_values_not_negative('regulation_trapeziums', ['max_availability', 'enablement_min',
                                       'low_break_point', 'high_break_point', 'enablement_max'], arg=1)
    def set_energy_and_regulation_capacity_constraints(self, regulation_trapeziums):
        """Creates constraints to ensure there is adequate capacity for regulation and energy dispatch targets.

        Create two constraints for each regulation services, one ensures operation on upper slope of the fcas
        regulation trapezium is consistent with energy dispatch, the second ensures operation on lower slope of the
        fcas regulation trapezium is consistent with energy dispatch.

        The constraints are described in the
        :download:`FCAS MODEL IN NEMDE documentation section 6.3  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        Examples
        --------

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot(dispatch_interval=60)

        Define the FCAS regulation trapeziums.

        >>> regulation_trapeziums = pd.DataFrame({
        ... 'unit': ['A'],
        ... 'service': ['raise_reg'],
        ... 'max_availability': [60.0],
        ... 'enablement_min': [20.0],
        ... 'low_break_point': [40.0],
        ... 'high_break_point': [60.0],
        ... 'enablement_max': [80.0]})

        Set the joint capacity constraints.

        >>> simple_market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)

        TNow the market should have the constraints and their mapping to decision varibales.

        >>> print(simple_market.constraints_rhs_and_type['energy_and_regulation_capacity'])
          unit    service  constraint_id type   rhs
        0    A  raise_reg              0   <=  80.0
        0    A  raise_reg              1   >=  20.0

        >>> print(simple_market.constraint_to_variable_map['unit_level']['energy_and_regulation_capacity'])
           constraint_id unit    service  coefficient
        0              0    A     energy     1.000000
        0              0    A  raise_reg     0.333333
        0              1    A     energy     1.000000
        0              1    A  raise_reg    -0.333333

        Parameters
        ----------
        regulation_trapeziums : pd.DataFrame
            The FCAS trapeziums for the regulation services being offered.

            ================   ======================================================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit (as `str`)
            service            the regulation service being offered (as `str`)
            max_availability   the maximum volume of the contingency service in MW (as `np.float64`)
            enablement_min     the energy dispatch level at which the unit can begin to provide the
                               contingency service, in MW (as `np.float64`)
            low_break_point    the energy dispatch level at which the unit can provide the full
                               contingency service offered, in MW (as `np.float64`)
            high_break_point   the energy dispatch level at which the unit can no longer provide the
                               full contingency service offered, in MW (as `np.float64`)
            enablement_max     the energy dispatch level at which the unit can no longer begin
                               the contingency service, in MW (as `np.float64`)
            ================   ======================================================================

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit and service combination in regulation_trapeziums.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the columns 'unit', 'service', 'max_availability', 'enablement_min', 'low_break_point',
                'high_break_point' or 'enablement_max' from regulation_trapeziums.
            UnexpectedColumn
                If there are columns other than 'unit', 'service', 'max_availability', 'enablement_min',
                'low_break_point', 'high_break_point' or 'enablement_max' in regulation_trapeziums.
            ColumnValues
                If there are inf, null or negative values in the columns of type `np.float64`.
        """

        rhs_and_type, variable_map = \
            fcas_constraints.energy_and_regulation_capacity_constraints(regulation_trapeziums, self.next_constraint_id)
        self.constraints_rhs_and_type['energy_and_regulation_capacity'] = rhs_and_type
        self.constraint_to_variable_map['unit_level']['energy_and_regulation_capacity'] = variable_map
        self.next_constraint_id = max(rhs_and_type['constraint_id']) + 1

    @check.required_columns('interconnector_directions_and_limits',
                            ['interconnector', 'to_region', 'from_region', 'max', 'min'])
    @check.allowed_columns('interconnector_directions_and_limits',
                           ['interconnector', 'to_region', 'from_region', 'max', 'min'])
    @check.repeated_rows('interconnector_directions_and_limits', ['interconnector'])
    @check.column_data_types('interconnector_directions_and_limits',
                             {'interconnector': str, 'to_region': str, 'from_region': str, 'max': np.float64,
                              'min': np.float64})
    @check.column_values_must_be_real('interconnector_directions_and_limits', ['min', 'max'])
    def set_interconnectors(self, interconnector_directions_and_limits):
        """Create lossless links between specified regions.

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        Import required packages.

        >>> import pandas as pd
        >>> from nempy import markets

        Initialise the market instance.

        >>> simple_market = markets.Spot()

        Define a an interconnector between NSW and VIC so generator can A can be used to meet demand in VIC.

        >>> interconnector = pd.DataFrame({
        ...     'interconnector': ['inter_one'],
        ...     'to_region': ['VIC'],
        ...     'from_region': ['NSW'],
        ...     'max': [100.0],
        ...     'min': [-100.0]})

        Create the interconnector.

        >>> simple_market.set_interconnectors(interconnector)

        The market should now have a decision variable defined for each interconnector.

        >>> print(simple_market.decision_variables['interconnectors'])
          interconnector  variable_id  lower_bound  upper_bound        type
        0      inter_one            0       -100.0        100.0  continuous

        ... and a mapping of those variables to to regional energy constraints.

        >>> print(simple_market.variable_to_constraint_map['regional']['interconnectors'])
           variable_id region service  coefficient
        0            0    VIC  energy          1.0
        1            0    NSW  energy         -1.0

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
            ==============  =====================================================================================

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

        # Create unit variable ids and map variables to regional constraints
        self.decision_variables['interconnectors'], self.variable_to_constraint_map['regional']['interconnectors'] \
            = inter.create(interconnector_directions_and_limits, self.next_variable_id)

        self.next_variable_id = max(self.decision_variables['interconnectors']['variable_id']) + 1

    @check.interconnectors_exist
    @check.required_columns('loss_functions', ['interconnector', 'from_region_loss_share', 'loss_function'], arg=1)
    @check.allowed_columns('loss_functions', ['interconnector', 'from_region_loss_share', 'loss_function'], arg=1)
    @check.repeated_rows('loss_functions', ['interconnector'], arg=1)
    @check.column_data_types('loss_functions', {'interconnector': str, 'from_region_loss_share': np.float64,
                                                'loss_function': 'callable'}, arg=1)
    @check.column_values_must_be_real('loss_functions', ['break_point'], arg=1)
    @check.column_values_outside_range('loss_functions', {'from_region_loss_share': [0.0, 1.0]}, arg=1)
    @check.required_columns('interpolation_break_point', ['interconnector', 'break_point'], arg=2)
    @check.allowed_columns('interpolation_break_point', ['interconnector', 'break_point'], arg=2)
    @check.repeated_rows('interpolation_break_point', ['interconnector', 'break_point'], arg=2)
    @check.column_data_types('interpolation_break_point', {'interconnector': str, 'break_point': np.float64}, arg=2)
    @check.column_values_must_be_real('interpolation_break_point', ['break_point'], arg=2)
    def set_interconnector_losses(self, loss_functions, interpolation_break_points):
        """Creates linearised loss functions for interconnectors.

        Creates a loss variable for each interconnector, this variable models losses by adding demand to each region.
        The losses are proportioned to each region according to the from_region_loss_share. In a region with one
        interconnector, where the region is the nominal from region, the impact on the demand constraint would be:

            generation - interconnector flow - interconnector losses * from_region_loss_share = demand

        If the region was the nominal to region, then:

            generation + interconnector flow - interconnector losses *  (1 - from_region_loss_share) = demand

        The loss variable is constrained to be a linear interpolation of the loss function between the two break points
        either side of to the actual line flow. This is achieved using a type 2 Special ordered set, where each
        variable is bound between 0 and 1, only 2 variables can be greater than 0 and all variables must sum to 1.
        The actual loss function is evaluated at each break point, the variables of the special order set are
        constrained such that their values weight the distance of the actual flow from the break points on either side
        e.g. If we had 3 break points at -100 MW, 0 MW and 100 MW, three weight variables w1, w2, and w3,
        and a loss function f, then the constraints would be of the form.

        Constrain the weight variables to sum to one:

            w1 + w2 + w3 = 1

        Constrain the weight variables give the relative weighting of adjacent breakpoint:

            w1 * -100.0 + w2 * 0.0 + w3 * 100.0 = interconnector flow

        Constrain the interconnector losses to be the weighted sum of the losses at the adjacent break point:

            w1 * f(-100.0) + w2 * f(0.0) + w3 * f(100.0) = interconnector losses

        Examples
        --------
        This is an example of the minimal set of steps for using this method.

        >>> import pandas as pd
        >>> from nempy import markets

        Create a market instance.

        >>> simple_market = markets.Spot()

        Create the interconnector, this need to be done before a interconnector losses can be set.

        >>> interconnectors = pd.DataFrame({
        ...    'interconnector': ['little_link'],
        ...    'to_region': ['VIC'],
        ...    'from_region': ['NSW'],
        ...    'max': [100.0],
        ...    'min': [-120.0]})

        >>> simple_market.set_interconnectors(interconnectors)

        Define the interconnector loss function. In this case losses are always 5 % of line flow.

        >>> def constant_losses(flow):
        ...     return abs(flow) * 0.05

        Define the function on a per interconnector basis. Also details how the losses should be proportioned to the
        connected regions.

        >>> loss_functions = pd.DataFrame({
        ...    'interconnector': ['little_link'],
        ...    'from_region_loss_share': [0.5],  # losses are shared equally.
        ...    'loss_function': [constant_losses]})

        Define The points to linearly interpolate the loss function between. In this example the loss function is
        linear so only three points are needed, but if a non linear loss function was used then more points would
        result in a better approximation.

        >>> interpolation_break_points = pd.DataFrame({
        ...    'interconnector': ['little_link', 'little_link', 'little_link'],
        ...    'break_point': [-120.0, 0.0, 100]})

        >>> simple_market.set_interconnector_losses(loss_functions, interpolation_break_points)

        The market should now have a decision variable defined for each interconnector's losses.

        >>> print(simple_market.decision_variables['interconnector_losses'])
          interconnector  variable_id  lower_bound  upper_bound        type
        0    little_link            1       -120.0        120.0  continuous

        ... and a mapping of those variables to regional energy constraints.

        >>> print(simple_market.variable_to_constraint_map['regional']['interconnector_losses'])
           variable_id region service  coefficient
        0            1    VIC  energy         -0.5
        1            1    NSW  energy         -0.5

        The market will also have a special ordered set of weight variables for interpolating the loss function
        between the break points.

        >>> print(simple_market.decision_variables['interpolation_weights'].loc[:,
        ...       ['interconnector', 'break_point', 'variable_id']])
          interconnector  break_point  variable_id
        0    little_link       -120.0            2
        1    little_link          0.0            3
        2    little_link        100.0            4

        >>> print(simple_market.decision_variables['interpolation_weights'].loc[:,
        ...       ['variable_id', 'lower_bound', 'upper_bound', 'type']])
           variable_id  lower_bound  upper_bound        type
        0            2          0.0          1.0  continuous
        1            3          0.0          1.0  continuous
        2            4          0.0          1.0  continuous

        and a set of constraints that implement the interpolation, see above explanation.

        >>> print(simple_market.constraints_rhs_and_type['interpolation_weights'])
          interconnector  constraint_id type  rhs
        0    little_link              0    =  1.0

        >>> print(simple_market.constraints_dynamic_rhs_and_type['link_loss_to_flow'])
          interconnector  constraint_id type  rhs_variable_id
        0    little_link              1    =                0
        0    little_link              2    =                1

        >>> print(simple_market.lhs_coefficients)
           variable_id  constraint_id  coefficient
        0            2              0          1.0
        1            3              0          1.0
        2            4              0          1.0
        0            2              1       -120.0
        1            3              1          0.0
        2            4              1        100.0
        0            2              2          6.0
        1            3              2          0.0
        2            4              2          5.0


        Parameters
        ----------
        loss_functions : pd.DataFrame

            ======================  ==============================================================================
            Columns:                Description:
            interconnector          unique identifier of a interconnector (as `str`)
            from_region_loss_share  The fraction of loss occuring in the from region, 0.0 to 1.0 (as `np.float64`)
            loss_function           A function that takes a flow, in MW as a float and returns the losses in MW
                                    (as `callable`)
            ======================  ==============================================================================

        interpolation_break_points : pd.DataFrame

            ==============  ============================================================================================
            Columns:        Description:
            interconnector  unique identifier of a interconnector (as `str`)
            break_point     points between which the loss function will be linearly interpolated, in MW
                            (as `np.float64`)
            ==============  ============================================================================================

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If all the interconnectors in the input data have not already been added to the model.
            RepeatedRowError
                If there is more than one row for any interconnector in loss_functions. Or if there is a repeated break
                point for an interconnector in interpolation_break_points.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If any columns are missing.
            UnexpectedColumn
                If there are any additional columns in the input DataFrames.
            ColumnValues
                If there are inf or null values in the numeric columns of either input DataFrames. Or if
                from_region_loss_share are outside the range of 0.0 to 1.0
        """

        # Create loss variables.
        loss_variables, loss_variables_constraint_map = \
            inter.create_loss_variables(self.decision_variables['interconnectors'],
                                        self.variable_to_constraint_map['regional']['interconnectors'],
                                        loss_functions, self.next_variable_id)
        next_variable_id = loss_variables['variable_id'].max() + 1

        # Create weight variables.
        weight_variables = inter.create_weights(interpolation_break_points, next_variable_id)

        # Creates weights sum constraint.
        weights_sum_lhs, weights_sum_rhs = inter.create_weights_must_sum_to_one(weight_variables,
                                                                                self.next_constraint_id)
        next_constraint_id = weights_sum_rhs['constraint_id'].max() + 1

        # Link weights to interconnector flow.
        link_to_flow_lhs, link_to_flow_rhs = inter.link_weights_to_inter_flow(weight_variables,
                                                                              self.decision_variables[
                                                                                  'interconnectors'],
                                                                              next_constraint_id)
        next_constraint_id = link_to_flow_rhs['constraint_id'].max() + 1

        # Link the losses to the interpolation weights.
        link_to_loss_lhs, link_to_loss_rhs = \
            inter.link_inter_loss_to_interpolation_weights(weight_variables, loss_variables, loss_functions,
                                                           next_constraint_id)

        # Combine lhs sides, note these are complete lhs and don't need to be mapped to constraints.
        lhs = pd.concat([weights_sum_lhs, link_to_flow_lhs, link_to_loss_lhs])

        # Combine constraints with a dynamic rhs i.e. a variable on the rhs.
        dynamic_rhs = pd.concat([link_to_flow_rhs, link_to_loss_rhs])

        # Save results.
        self.decision_variables['interconnector_losses'] = loss_variables
        self.variable_to_constraint_map['regional']['interconnector_losses'] = loss_variables_constraint_map
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

        constraints_lhs = self.lhs_coefficients

        if len(self.constraint_to_variable_map['regional']) > 0:
            regional_constraints_lhs = create_lhs.create(self.constraint_to_variable_map['regional'],
                                                         self.variable_to_constraint_map['regional'],
                                                         ['region', 'service'])

            constraints_lhs = pd.concat([constraints_lhs, regional_constraints_lhs])

        if len(self.constraint_to_variable_map['unit_level']) > 0:
            unit_constraints_lhs = create_lhs.create(self.constraint_to_variable_map['unit_level'],
                                                     self.variable_to_constraint_map['unit_level'],
                                                     ['unit', 'service'])
            constraints_lhs = pd.concat([constraints_lhs, unit_constraints_lhs])

        decision_variables, market_constraints_rhs_and_type = solver_interface.dispatch(
            self.decision_variables, constraints_lhs, self.constraints_rhs_and_type,
            self.market_constraints_rhs_and_type, self.constraints_dynamic_rhs_and_type,
            self.objective_function_components)
        self.market_constraints_rhs_and_type = market_constraints_rhs_and_type
        self.decision_variables = decision_variables

    def get_unit_dispatch(self):
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

        >>> simple_market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_price_bids(price_bids)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create unit capacity based constraints.

        >>> simple_market.set_demand_constraints(demand)

        Call the dispatch method.

        >>> simple_market.dispatch()

        Now the market dispatch can be retrieved.

        >>> print(simple_market.get_unit_dispatch())
          unit service  dispatch
        0    A  energy      45.0
        1    B  energy      55.0

        Returns
        -------
        pd.DataFrame

        Raises
        ------
            ModelBuildError
                If a model build process is incomplete, i.e. there are energy bids but not energy demand set.
        """
        dispatch = self.decision_variables['bids'].loc[:, ['unit', 'service', 'value']]
        dispatch.columns = ['unit', 'service', 'dispatch']
        return dispatch.groupby(['unit', 'service'], as_index=False).sum()

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

        >>> simple_market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_price_bids(price_bids)

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
        prices = self.market_constraints_rhs_and_type['demand'].loc[:, ['region', 'price']]
        return prices

    def get_fcas_prices(self):
        """Retrives the price associated with each set of FCAS requirement constraints.

        Returns
        -------
        pd.DateFrame
        """
        prices = self.market_constraints_rhs_and_type['fcas'].loc[:, ['set', 'price']]
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

        >>> simple_market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [80.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> simple_market.set_unit_price_bids(price_bids)

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

        >>> print(simple_market.get_unit_dispatch())
          unit service  dispatch
        0    A  energy      90.0

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
        flow = self.decision_variables['interconnectors'].loc[:, ['interconnector', 'value']]
        flow.columns = ['interconnector', 'flow']
        if 'interconnector_losses' in self.decision_variables:
            losses = self.decision_variables['interconnector_losses'].loc[:, ['interconnector', 'value']]
            losses.columns = ['interconnector', 'losses']
            flow = pd.merge(flow, losses, 'left', on='interconnector')

        return flow.reset_index(drop=True)
