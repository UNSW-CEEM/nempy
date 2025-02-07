import numpy as np
import pandas as pd

from nempy.help_functions import helper_functions as hf
from nempy.spot_market_backend import elastic_constraints, fcas_constraints, interconnectors as inter, \
    market_constraints, objective_function, solver_interface, unit_constraints, variable_ids, check, \
    dataframe_validator as dv, ramp_rate_processing as rrp

pd.set_option('display.width', None)


# noinspection PyProtectedMember
class SpotMarket:
    """Class for constructing and dispatching the spot market on an interval basis.

    Note: bidirectional units are defined by including the unit twice in the unit_info
    input, once with the dispatch type "generator" and once with the type "load". Then
    energy and FCAS regulation bids (and FCAS trapezium paramters) can be provided for
    both the generation and the load components of the unit. Note only a single set of
    bids can be provided for FCAS contigency, these should be given the dispatch_type
    "generator", but both the load and generator side can contribute to delivery.


    Examples
    --------
    Define the unit information data needed to initialise the market, in this example all units are in the same
    region.

    >>> unit_info = pd.DataFrame({
    ...     'unit': ['A', 'B'],
    ...     'region': ['NSW', 'NSW']})

    Initialise the market instance.

    >>> market = SpotMarket(market_regions=['NSW'],
    ...                            unit_info=unit_info)

    The units are given a default dispatch_type and loss_factor. Note this data is stored in a private method and
    not intended for public use.

    >>> market._unit_info
      unit region  loss_factor dispatch_type
    0    A    NSW          1.0     generator
    1    B    NSW          1.0     generator


    Parameters
    ----------
    market_regions : list[str]
        The market regions, used to validate inputs.

    unit_info : pd.DataFrame
        Information on a unit basis, not all columns are required.

        =============  ===============================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit, \n
                       (as `str`)
        region         location of unit, required (as `str`)
        dispatch_type  "load" or "generator", optional default \n
                       'generator', if provided for unit_info must also \n
                       be provided other input tables with 'unit' column \n
                       except for uigf limits and fast start profiles.
        loss_factor    marginal, average or combined loss factors, \n
                       :download:`see AEMO doc <../../docs/pdfs/Treatment_of_Loss_Factors_in_the_NEM.pdf>`, \n
                       optional, (as `np.int64`)
        =============  ===============================================

    dispatch_interval : int
        The length of the dispatch interval in minutes, used for interpreting ramp rates.

    Attributes
    ----------
    solver_name : str
        The solver to use must be one of solver options of the mip-python package that is used to interface to solvers.
        Currently, the only supported solvers are CBC and Gurobi, so allowed solver names are 'CBC' and 'GUROBI'. Default
        value is CBC, CBC works out of the box after installing Nempy, but Gurobi must be installed separately.

    Raises
    ------
        RepeatedRowError
            If there is more than one row for any 'unit'.
        ColumnDataTypeError
            If columns are not of the required type.
        MissingColumnError
            If the column 'units' or 'regions' is missing.
        UnexpectedColumn
            There is a column that is not 'units', 'regions', 'dispatch_type' or 'loss_factor'.
        ColumnValues
            If there are inf, null or negative values in the 'loss_factor' column."""

    def __init__(self, market_regions, unit_info, dispatch_interval=5):
        self.dispatch_interval = dispatch_interval
        self._unit_info = None
        self._decision_variables = {}
        self._variable_to_constraint_map = {'regional': {}, 'unit_level': {}}
        self._constraint_to_variable_map = {'regional': {}, 'unit_level': {}}
        self._lhs_coefficients = {}
        self._generic_constraint_lhs = {}
        self._constraints_rhs_and_type = {}
        self._constraints_dynamic_rhs_and_type = {}
        self._market_constraints_rhs_and_type = {}
        self._objective_function_components = {}
        self._interconnector_directions = None
        self._interconnector_loss_shares = None
        self._next_variable_id = 0
        self._next_constraint_id = 0
        self.validate_inputs = True
        self.check = True
        self._market_regions = market_regions
        self._allowed_dispatch_types = ['generator', 'load']
        self._allowed_services = ['energy', 'raise_reg', 'lower_reg', 'raise_5min', 'lower_5min', 'raise_60s',
                                  'lower_60s', 'raise_6s', 'lower_6s', 'raise_1s', 'lower_1s']
        self._allowed_fcas_services = self._allowed_services[:]
        self._allowed_fcas_services.remove('energy')
        self._allowed_contingency_fcas_services = self._allowed_fcas_services[:]
        self._allowed_contingency_fcas_services.remove('raise_reg')
        self._allowed_contingency_fcas_services.remove('lower_reg')
        self._allowed_regulation_fcas_services = ['raise_reg', 'lower_reg']
        self._allowed_constraint_types = ['<=', '=', '>=']
        self.solver_name = 'CBC'
        self.objective_value = None

        if 'loss_factor' not in unit_info.columns:
            unit_info['loss_factor'] = 1.0

        if 'dispatch_type' not in unit_info.columns:
            self.dispatch_type_required = False
            unit_info['dispatch_type'] = 'generator'
        else:
            self.dispatch_type_required = True

        if self.validate_inputs:
            self._validate_unit_info(unit_info)

        self._unit_info = unit_info

        units = unit_info['unit'].value_counts()
        self._bidirectional_units = units[units == 2].index.tolist()

    def _validate_unit_info(self, unit_info):
        schema = dv.DataFrameSchema(name='unit_info', primary_keys=['unit', 'dispatch_type'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str))
        schema.add_column(dv.SeriesSchema(name='region', data_type=str, allowed_values=self._market_regions))
        schema.add_column(dv.SeriesSchema(name='loss_factor', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.add_column(dv.SeriesSchema(name='dispatch_type', data_type=str, allowed_values=['generator', 'load']))
        schema.validate(unit_info)

    def set_unit_volume_bids(self, volume_bids):
        """Creates the decision variables corresponding to unit bids.

        Variables are created by reserving a variable id (as `int`) for each bid. Bids with a volume of 0 MW do not
        have a variable created. The lower bound of the variables are set to zero and the upper bound to the bid
        volume, the variable type is set to continuous. If service is not specified for the bids they are given the
        default service value of 'energy'. If dispatch_type is not specified for the bids they are given the
        default value of 'generator'.

        Examples
        --------

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                            unit_info=unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 0.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        The market should now have the variables.

        >>> print(market._decision_variables['bids'])
          unit capacity_band service dispatch_type  variable_id  lower_bound  upper_bound        type
        0    A             1  energy     generator            0          0.0         20.0  continuous
        1    A             2  energy     generator            1          0.0         20.0  continuous
        2    A             3  energy     generator            2          0.0          5.0  continuous
        3    B             1  energy     generator            3          0.0         50.0  continuous
        4    B             2  energy     generator            4          0.0         30.0  continuous

        A mapping of these variables to constraints acting on that unit and service should also exist.

        >>> print(market._variable_to_constraint_map['unit_level']['bids'])
           variable_id unit service dispatch_type  coefficient
        0            0    A  energy     generator          1.0
        1            1    A  energy     generator          1.0
        2            2    A  energy     generator          1.0
        3            3    B  energy     generator          1.0
        4            4    B  energy     generator          1.0

        A mapping of these variables to constraints acting on the units region and service should also exist.

        >>> print(market._variable_to_constraint_map['regional']['bids'])
           variable_id region service dispatch_type  coefficient
        0            0    NSW  energy     generator          1.0
        1            1    NSW  energy     generator          1.0
        2            2    NSW  energy     generator          1.0
        3            3    NSW  energy     generator          1.0
        4            4    NSW  energy     generator          1.0

        Parameters
        ----------
        volume_bids : pd.DataFrame
            Bids by unit, in MW, can contain up to 10 bid bands, these should be labeled '1' to '10'.

            =============  ================================================
            Columns:       Description:
            unit           unique identifier of a dispatch unit (as `str`)
            service        the service being provided, optional, \n
                           default 'energy', (as `str`)
            dispatch_type  "load" or "generator", must be provided if \n
                           given in unit_info and cannot be provided \n
                           if not given in unit_info. Defaults to \n
                           'generator'.
                           be provided for price bids and vise a versa, (as `str`)
            1              bid volume in the 1st band, in MW, \n
                           (as `np.float64`)
            2              bid volume in the 2nd band, in MW, optional, \n
                           (as `np.float64`)
            :
            10             bid volume in the nth band, in MW, optional, \n
                           (as `np.float64`)
            ========       ================================================

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit and service combination.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the column 'units' is missing or there are no bid bands.
            UnexpectedColumn
                There is a column that is not 'unit', 'service' or '1' to '10'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
        """
        if self.validate_inputs:
            self._validate_volume_bids(volume_bids)

        self._validate_bidirectional_unit_inputs(volume_bids, "volume_bids")

        self._decision_variables['bids'], variable_to_unit_level_constraint_map, variable_to_regional_constraint_map, = \
            variable_ids.bids(volume_bids, self._unit_info, self._next_variable_id, self._bidirectional_units)

        self._variable_to_constraint_map['regional']['bids'] = variable_to_regional_constraint_map
        self._variable_to_constraint_map['unit_level']['bids'] = variable_to_unit_level_constraint_map
        self._next_variable_id = max(self._decision_variables['bids']['variable_id']) + 1

    def _validate_volume_bids(self, volume_bids):
        schema = dv.DataFrameSchema(name='volume_bids', primary_keys=['unit', 'service', 'dispatch_type'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        if self.dispatch_type_required:
            schema.add_column(dv.SeriesSchema(name='dispatch_type', data_type=str, allowed_values=['generator', 'load']))
        schema.add_column(dv.SeriesSchema(name='service', data_type=str, allowed_values=self._allowed_services),
                          optional=True)
        schema.add_column(dv.SeriesSchema(name=str(1), data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        for bid_band in range(2, 11):
            schema.add_column(dv.SeriesSchema(name=str(bid_band), data_type=np.float64, must_be_real_number=True,
                                              not_negative=True), optional=True)
        schema.validate(volume_bids)

    def _validate_bidirectional_unit_inputs(self, inputs, input_name):
        """Check that if bidirectional unit where defined in unit_info when initialising the market that the inputs
        provided for validation entry for both the generator and the load side of each bidirectional unit.
        """
        if "service" in inputs.columns:
            inputs = inputs[inputs["service"]=="energy"].copy()
        inputs = inputs[inputs["unit"].isin(self._bidirectional_units)].copy()
        if len(inputs['unit']) != len(set(inputs['unit'])) * 2:
            raise ModelBuildError(f"Invalid inputs for {input_name} a generator and load input was not provided for each "
                                  f"bidirectional unit.")

    def set_unit_price_bids(self, price_bids):
        """Creates the objective function costs corresponding to energy bids.

        If no loss factors have been provided as part of the unit information when the model was initialised then the
        costs in the objective function are as bid. If loss factors are provided then the bid costs are referred to the
        regional reference node by dividing by the loss factor. If service is not specified for the bids they are given
        the default service value of 'energy'. If dispatch_type is not specified for the bids they are given the
        default value of 'generator'.

        Examples
        --------

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids. Bids for each unit need to be monotonically increasing.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the energy bids.

        >>> market.set_unit_price_bids(price_bids)

        The variable assocaited with each bid should now have a cost.

        >>> print(market._objective_function_components['bids'])
           variable_id unit service dispatch_type capacity_band   cost
        0            0    A  energy     generator             1   50.0
        1            1    A  energy     generator             2  100.0
        2            2    A  energy     generator             3  100.0
        3            3    B  energy     generator             1  100.0
        4            4    B  energy     generator             2  130.0
        5            5    B  energy     generator             3  150.0

        Parameters
        ----------
        price_bids : pd.DataFrame
            Bids by unit, in $/MW, can contain up to 10 bid bands.

            =============  ================================================
            Columns:       Description:
            unit           unique identifier of a dispatch unit (as `str`)
            service        the service being provided, optional,
                           default 'energy', (as `str`)
            dispatch_type  "load" or "generator", must be provided if \n
                           given in unit_info and cannot be provided \n
                           if not given in unit_info. Defaults to \n
                           'generator'.
            1              bid price in the 1st band, in $/MW, \n
                           (as `np.float64`)
            2              bid price in the 2nd band, in $/MW, optional, \n
                           (as `np.float64`)
            :
            10             bid price in the nth band, in $/MW, optional, \n
                           (as `np.float64`)
            ============   ================================================

        Returns
        -------
        None

        Raises
        ------
            ModelBuildError
                If the volume bids have not been set yet.
            RepeatedRowError
                If there is more than one row for any unit and service combination.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the column 'units' is missing or there are no bid bands.
            UnexpectedColumn
                There is a column that is not 'units', 'region' or '1' to '10'.
            ColumnValues
                If there are inf, -inf or null values in the bid band columns.
            BidsNotMonotonicIncreasing
                If the bids band price for all units are not monotonic increasing.
        """
        self._check_unit_volume_bids_set()
        self._validate_price_bids(price_bids)
        self._validate_bidirectional_unit_inputs(price_bids, "price_bids")
        energy_objective_function = objective_function.bids(self._decision_variables['bids'], price_bids)
        energy_objective_function = objective_function.scale_by_loss_factors(energy_objective_function, self._unit_info)
        self._objective_function_components['bids'] = \
            energy_objective_function.loc[:, ['variable_id', 'unit', 'service', 'dispatch_type', 'capacity_band',
                                              'cost']]

    def _validate_price_bids(self, price_bids):
        schema = dv.DataFrameSchema(name='price_bids', primary_keys=['unit', 'service', 'dispatch_type'],
                                    row_monatonic_increasing=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='service', data_type=str, allowed_values=self._allowed_services),
                          optional=True)
        if self.dispatch_type_required:
            schema.add_column(dv.SeriesSchema(name='dispatch_type', data_type=str, allowed_values=['generator', 'load']))
        schema.add_column(dv.SeriesSchema(name=str(1), data_type=np.float64, must_be_real_number=True))
        for bid_band in range(2, 11):
            schema.add_column(dv.SeriesSchema(name=str(bid_band), data_type=np.float64, must_be_real_number=True),
                              optional=True)
        schema.validate(price_bids)

    def _check_unit_volume_bids_set(self):
        if 'bids' not in self._decision_variables:
            raise ModelBuildError('Price bids cannot be set before setting volume bids.')

    def set_unit_bid_capacity_constraints(self, unit_limits, violation_cost=None):
        """Creates constraints that limit unit output based on their bid in max capacity. If a unit bids in zero
        volume then a constraint is not created.

        Examples
        --------

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of unit capacities.

        >>> unit_limits = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'capacity': [60.0, 100.0]})

        Create unit capacity based constraints.

        >>> market.set_unit_bid_capacity_constraints(unit_limits)

        The market should now have a set of constraints.

        >>> print(market._constraints_rhs_and_type['unit_bid_capacity'])
          unit service dispatch_type  constraint_id type    rhs
        0    A  energy     generator              0   <=   60.0
        1    B  energy     generator              1   <=  100.0

        ... and a mapping of those constraints to the variable types on the lhs.

        >>> unit_mapping = market._constraint_to_variable_map['unit_level']

        >>> print(unit_mapping['unit_bid_capacity'])
           constraint_id unit service dispatch_type  coefficient
        0              0    A  energy     generator          1.0
        1              1    B  energy     generator          1.0


        Parameters
        ----------
        unit_limits : pd.DataFrame
            Capacity by unit.

            =============  =====================================================
            Columns:       Description:
            unit           unique identifier of a dispatch unit (as `str`)
            dispatch_type  "load" or "generator", must be provided if \n
                           given in unit_info and cannot be provided \n
                           if not given in unit_info. Defaults to \n
                           'generator'.
            capacity       The maximum output of the unit if unconstrained \n
                           by ramp rate, in MW (as `np.float64`)
            =============  ======================================================

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

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
                If columns are not of the required types.
            MissingColumnError
                If the column 'units' or 'capacity' is missing.
            UnexpectedColumn
                There is a column that is not 'units' or 'capacity'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
        """
        self._check_unit_volume_bids_set()
        self._validate_unit_limits(unit_limits)
        rhs_and_type, variable_map = unit_constraints.capacity(
            unit_limits,
            self._next_constraint_id,
            self._bidirectional_units
        )
        self._constraints_rhs_and_type['unit_bid_capacity'] = rhs_and_type
        self._constraint_to_variable_map['unit_level']['unit_bid_capacity'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('unit_bid_capacity', violation_cost)

    def _validate_unit_limits(self, unit_limits):
        schema = dv.DataFrameSchema(name='unit_limits', primary_keys=['unit', 'dispatch_type'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        if self.dispatch_type_required:
            schema.add_column(dv.SeriesSchema(name='dispatch_type', data_type=str, allowed_values=['generator', 'load']))
        schema.add_column(dv.SeriesSchema(name='capacity', data_type=np.float64))
        schema.validate(unit_limits)

    def set_unconstrained_intermittent_generation_forecast_constraint(self, unit_limits, violation_cost=None):
        """Creates constraints that limit unit output based on their forecast output.

        Note: All semi-scheduled units are assumed not to be bidirectional.

        Examples
        --------

        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of unit forecast capacities.

        >>> unit_limits = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'capacity': [60.0, 100.0]})

        Create unit capacity based constraints.

        >>> market.set_unconstrained_intermittent_generation_forecast_constraint(unit_limits)

        The market should now have a set of constraints.

        >>> print(market._constraints_rhs_and_type['uigf_capacity'])
          unit service dispatch_type  constraint_id type    rhs
        0    A  energy     generator              0   <=   60.0
        1    B  energy     generator              1   <=  100.0

        ... and a mapping of those constraints to the variable types on the lhs.

        >>> unit_mapping = market._constraint_to_variable_map['unit_level']

        >>> print(unit_mapping['uigf_capacity'])
           constraint_id unit service dispatch_type  coefficient
        0              0    A  energy     generator          1.0
        1              1    B  energy     generator          1.0


        Parameters
        ----------
        unit_limits : pd.DataFrame
            Capacity by unit.

            ========  ================================================
            Columns:  Description:
            unit      unique identifier of a dispatch unit (as `str`)
            capacity  The maximum output of the unit if unconstrained \n
                      by ramp rate, in MW (as `np.float64`)
            ========  ================================================

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

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
                If columns are not of the required type.
            MissingColumnError
                If the column 'units' or 'capacity' is missing.
            UnexpectedColumn
                There is a column that is not 'units' or 'capacity'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
        """
        self._check_unit_volume_bids_set()
        self._validate_uigf_limits(unit_limits)
        rhs_and_type, variable_map = unit_constraints.uigf(unit_limits, self._next_constraint_id)
        self._constraints_rhs_and_type['uigf_capacity'] = rhs_and_type
        self._constraint_to_variable_map['unit_level']['uigf_capacity'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('uigf_capacity', violation_cost)

    def _validate_uigf_limits(self, unit_limits):
        schema = dv.DataFrameSchema(name='unit_limits', primary_keys=['unit'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='capacity', data_type=np.float64))
        schema.validate(unit_limits)

    def set_unit_ramp_rate_constraints(self, ramp_details, scada_ramp_rates=None, fast_start_profiles=None,
                                       run_type='no_fast_start_units', violation_cost=None):
        """Creates constraints on unit output based on ramp rates.

        1. For bidirectional units composite ramp rates are caclauted as per :download:`see AEMO doc <../../docs/pdfs/SO_OP_3705 Dispatch.pdf>`
        2. If scada ramp rates are provided then the lesser of the as bid and scada ramp rates are used.
        3. If fast_start_profiles are provided ramps rates are adjusted to account for the fast start profiles.

        Constrains the unit output to be: target <= initial_output + ramp_up_rate * (dispatch_interval / 60) and
        target >= initial_output - ramp_down_rate * (dispatch_interval / 60).


        Examples
        --------
        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info,
        ...                     dispatch_interval=30)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of unit ramp up rates.

        >>> ramp_details = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'initial_output': [20.0, 50.0],
        ...     'ramp_up_rate': [30.0, 100.0],
        ...     'ramp_down_rate': [30.0, 100.0]})

        Create unit capacity based constraints.

        >>> market.set_unit_ramp_rate_constraints(ramp_details)

        The market should now have a set of constraints.

        >>> print(market._constraints_rhs_and_type['ramp_up'])
          unit service dispatch_type  constraint_id type    rhs
        0    A  energy     generator              0   <=   35.0
        1    B  energy     generator              1   <=  100.0


        >>> print(market._constraints_rhs_and_type['ramp_down'])
          unit service dispatch_type  constraint_id type  rhs
        0    A  energy     generator              2   >=  5.0
        1    B  energy     generator              3   >=  0.0

        ... and a mapping of those constraints to variable type for the lhs.

        >>> unit_mapping = market._constraint_to_variable_map['unit_level']

        >>> print(unit_mapping['ramp_up'])
           constraint_id unit service dispatch_type  coefficient
        0              0    A  energy     generator          1.0
        1              1    B  energy     generator          1.0

        >>> print(unit_mapping['ramp_down'])
           constraint_id unit service dispatch_type  coefficient
        0              2    A  energy     generator          1.0
        1              3    B  energy     generator          1.0

        Parameters
        ----------
        ramp_details : pd.DataFrame

            ===================   ==========================================
            Columns:              Description:
            unit                  unique identifier of a dispatch unit, \n
                                  (as `str`)
            dispatch_type         "load" or "generator", must be provided if \n
                                  given in unit_info and cannot be provided \n
                                  if not given in unit_info. Defaults to \n
                                 'generator'.
            initial_output        the output of the unit at the start of \n
                                  the dispatch interval, in MW, \n
                                  (as `np.float64`)
            ramp_up_rate          the as bid maximum rate at which the unit can \n
                                  increase output, in MW/h, (as `np.float64`)
            ramp_down_rate        the as bid maximum rate at which the unit can \n
                                  decrease output, in MW/h, (as `np.float64`)

        scada_ramp_rates : pd.DataFrame

            Bidirectional units share scada ramp rates so only one set is given per unit and there is no need for the
            dispatch_type to be specified.

            ===================   ==========================================
            Columns:              Description:
            unit                  unique identifier of a dispatch unit, \n
                                  (as `str`)
            scada_ramp_up_rate    the scada maximum rate at which the unit can \n
                                  increase output, in MW/h, (as `np.float64`), \n
                                  optional
            scada_ramp_down_rate  the scada maximum rate at which the unit can \n
                                  decrease output, in MW/h, (as `np.float64`), \n
                                  optional.
            ====================  ==========================================


        fast_start_profiles : pd.DataFrame

            When run_type is set to 'no_fast_start_units': this table is not
                requried.

            When run_type is set to 'fast_start_first_run':

            ===================   ==========================================
            Columns:              Description:
            unit                  unique identifier of a dispatch unit, \n
                                  (as `str`)
            current_mode          The fast start operating mode of the unit \n
                                  at the start of the dispatch interval \n
                                  (as `int`)
            ====================  ==========================================

            When run_type is set to 'fast_start_first_run':

            ==========================   ==========================================
            Columns:                    Description:
            unit                        unique identifier of a dispatch unit, \n
                                        (as `str`)
            end_mode                    The fast start operating mode of the unit \n
                                        at the end of the dispatch interval \n
                                        (as `int`)
            time_since_end_of_mode_two  The number of minutes since the unit was \n
                                        operating in mode 2 in minutes, can be nan \n
                                        if the unit (as `int`)
            min_loading                 the fast start profile min loading in MW \n
                                        (as `foat`)
            ==========================  ==========================================

        run_type: str specifying whever this the fist fast start run or the second, or
            if fast start units are not being considered. One of 'no_fast_start_units',
            'fast_start_first_run', or 'fast_start_second_run'.

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

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
                If the column 'units', 'initial_output' or 'ramp_up_rate' is missing.
            UnexpectedColumn
                There is a column that is not 'units', 'initial_output' or 'ramp_up_rate'.
            ColumnValues
                If there are inf, null or negative values in the bid band columns.
        """
        self._validate_ramp_rates(ramp_details)
        self._validate_bidirectional_unit_inputs(ramp_details, "ramp_details")

        if run_type != "no_fast_start_units" and fast_start_profiles is not None and self.validate_inputs:
            self._validate_fast_start_profiles_for_ramp_rates(fast_start_profiles, run_type)

        if "dispatch_type" not in ramp_details.columns:
            ramp_details["dispatch_type"] = "generator"

        ramp_details, bidirectional_unit_ramp_details = rrp._calculate_composite_ramp_rates(
            ramp_rates=ramp_details,
            dispatch_interval=self.dispatch_interval,
            bidirectional_units=self._bidirectional_units
        )

        if scada_ramp_rates is not None:
            if self.validate_inputs:
                self._validate_scada_ramp_rates(scada_ramp_rates)
            ramp_details = rrp._adjust_for_scada_ramp_rates(ramp_details, scada_ramp_rates)
            bidirectional_unit_ramp_details = rrp._adjust_for_scada_ramp_rates(
                bidirectional_unit_ramp_details, scada_ramp_rates)

        ramp_details = rrp._adjust_ramp_rates_for_fast_start_profiles(
            ramp_details, run_type, fast_start_profiles, self.dispatch_interval
        )

        rhs_and_type, variable_map = unit_constraints.ramp_up(ramp_details, self._next_constraint_id,
                                                              self.dispatch_interval)
        self._constraints_rhs_and_type['ramp_up'] = rhs_and_type
        self._constraint_to_variable_map['unit_level']['ramp_up'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        rhs_and_type, variable_map = unit_constraints.ramp_down(ramp_details, self._next_constraint_id,
                                                              self.dispatch_interval)
        self._constraints_rhs_and_type['ramp_down'] = rhs_and_type
        self._constraint_to_variable_map['unit_level']['ramp_down'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if not bidirectional_unit_ramp_details.empty:

            rhs_and_type, variable_map = rrp._bidirectional_ramp_constraints(
                bidirectional_unit_ramp_details, 'up', self._next_constraint_id, self.dispatch_interval)
            self._constraints_rhs_and_type['bidirectional_ramp_up'] = rhs_and_type
            self._constraint_to_variable_map['unit_level']['bidirectional_ramp_up'] = variable_map
            self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

            rhs_and_type, variable_map = rrp._bidirectional_ramp_constraints(
                bidirectional_unit_ramp_details, 'down', self._next_constraint_id, self.dispatch_interval)
            self._constraints_rhs_and_type['bidirectional_ramp_down'] = rhs_and_type
            self._constraint_to_variable_map['unit_level']['bidirectional_ramp_down'] = variable_map
            self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('ramp_up', violation_cost)
            self.make_constraints_elastic('ramp_down', violation_cost)

            if 'bidirectional_ramp_up' in self._constraint_to_variable_map['unit_level'].keys():
                self.make_constraints_elastic('bidirectional_ramp_up', violation_cost)
            if 'bidirectional_ramp_down' in self._constraint_to_variable_map['unit_level'].keys():
                self.make_constraints_elastic('bidirectional_ramp_down', violation_cost)

    def _validate_ramp_rates(self, ramp_details):
        schema = dv.DataFrameSchema(name='ramp_details', primary_keys=['unit', 'dispatch_type'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='initial_output', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='ramp_up_rate', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='ramp_down_rate', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='scada_ramp_up_rate', data_type=np.float64),
                          optional=True)
        schema.add_column(dv.SeriesSchema(name='scada_ramp_down_rate', data_type=np.float64),
                          optional=True)
        if self.dispatch_type_required:
            schema.add_column(dv.SeriesSchema(name='dispatch_type', data_type=str, allowed_values=['generator', 'load']))
        schema.validate(ramp_details)

    def _validate_scada_ramp_rates(self, ramp_details):
        schema = dv.DataFrameSchema(name='ramp_details', primary_keys=['unit'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='scada_ramp_up_rate', data_type=np.float64),
                          optional=True)
        schema.add_column(dv.SeriesSchema(name='scada_ramp_down_rate', data_type=np.float64),
                          optional=True)
        schema.validate(ramp_details)

    def _validate_fast_start_profiles_for_ramp_rates(self, fast_start_profiles, run_type):
        schema = dv.DataFrameSchema(name='fast_start_profiles', primary_keys=['unit'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))

        if run_type == "fast_start_first_run":
            schema.add_column(dv.SeriesSchema(name='current_mode', data_type=np.int64, must_be_real_number=True,
                                              not_negative=True))
        elif run_type == "fast_start_second_run":
            schema.add_column(dv.SeriesSchema(name='end_mode', data_type=np.int64, must_be_real_number=True,
                                              not_negative=True))
            schema.add_column(dv.SeriesSchema(name='min_loading', data_type=np.float64, must_be_real_number=True,
                                              not_negative=True))
            schema.add_column(dv.SeriesSchema(name='time_since_end_of_mode_two', data_type=np.float64,
                                              not_negative=True))
        schema.validate(fast_start_profiles)

    def set_fast_start_constraints(self, fast_start_profiles, violation_cost=None):
        """Create the constraints on fast start units dispatch, :download:`see AEMO doc <../../docs/pdfs/Fast_Start_Unit_Inflexibility_Profile_Model_October_2014.pdf>`

        Note: All fast start type units are assumed not to be bidirectional.

        Examples
        --------
        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B', 'C', 'D', 'E'],
        ...     'region': ['NSW', 'NSW', 'NSW', 'NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info,
        ...                     dispatch_interval=30)

        Define some example fast start conditions.

        >>> fast_start_conditions = pd.DataFrame({
        ...     'unit': ['A', 'B', 'C', 'D', 'E'],
        ...     'end_mode': [0, 1, 2, 3, 4],
        ...     'time_in_end_mode': [4.0, 5.0, 5.0, 12.0, 10.0],
        ...     'mode_two_length': [7.0, 4.0, 10.0, 8.0, 6.0],
        ...     'mode_four_length': [10.0, 10.0, 20.0, 8.0, 20.0],
        ...     'min_loading': [30.0, 40.0, 35.0, 50.0, 60.0]})

        Add fast start constraints.

        >>> market.set_fast_start_constraints(fast_start_conditions)

        The market should now have a set of constraints.

        >>> print(market._constraints_rhs_and_type['fast_start'])
          unit service dispatch_type  constraint_id type   rhs
        0    A  energy     generator              0   <=   0.0
        1    B  energy     generator              1   <=   0.0
        0    C  energy     generator              2   >=  17.5
        0    C  energy     generator              3   <=  17.5
        0    D  energy     generator              4   >=  50.0
        0    E  energy     generator              5   >=  30.0

        ... and a mapping of those constraints to variable type for the lhs.

        >>> unit_mapping = market._constraint_to_variable_map['unit_level']

        >>> print(unit_mapping['fast_start'])
           constraint_id unit service dispatch_type  coefficient
        0              0    A  energy     generator          1.0
        1              1    B  energy     generator          1.0
        0              3    C  energy     generator          1.0
        0              2    C  energy     generator          1.0
        0              4    D  energy     generator          1.0
        0              5    E  energy     generator          1.0

        Parameters
        ----------
        fast_start_profiles : pd.DataFrame
            ================  ==========================================
            Columns:          Description:
            unit              unique identifier of a dispatch unit, \n
                              (as `str`)
            end_mode          the fast start dispatch mode the unit \n
                              will end the dispatch interval in, \n
                              in minutes, (as `np.int64`),
            time_in_end_mode  the time the unit will have spent in the \n
                              end mode at the end of this dispatch \n
                              interval, in minutes (as `np.int64`)
            mode_two_length   the length of dispatch mode 2 for the \n
                              unit, in minutes, (as `np.int64`)
            mode_four_length  the length of dispatch mode 4 for the \n
                              unit, in minutes, (as `np.int64`)
            min_loading       the minimum stable operating level of \n
                              unit, in MW, (as `np.float64`)
            ================  ==========================================

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

        Returns
        -------

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If any columns are missing.
            UnexpectedColumn
                If any additional columns are present.
            ColumnValues
                If there are inf, null or negative values in any of the numeric columns.

        """
        if self.validate_inputs:
            self._validate_fast_start_profiles(fast_start_profiles)

        fast_start_profiles = pd.merge(
            self._unit_info.loc[:, ['unit', 'dispatch_type']],
            fast_start_profiles,
            on=['unit']
        )

        rhs_and_type, variable_map = unit_constraints.create_fast_start_profile_constraints(
            fast_start_profiles, self._next_constraint_id)
        if not rhs_and_type.empty:
            self._constraints_rhs_and_type['fast_start'] = rhs_and_type
            self._constraint_to_variable_map['unit_level']['fast_start'] = variable_map
            self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1
            if violation_cost is not None:
                self.make_constraints_elastic('fast_start', violation_cost)

    def remove_fast_start_constraints(self):
        if 'fast_start' in self._constraints_rhs_and_type:
            del self._constraints_rhs_and_type['fast_start']
            del self._constraint_to_variable_map['unit_level']['fast_start']
        if 'fast_start_deficit' in self._decision_variables:
            del self._decision_variables['fast_start_deficit']
            del self._objective_function_components['fast_start_deficit']
            del self._lhs_coefficients['fast_start_deficit']

    def _validate_fast_start_profiles(self, fast_start_profiles):
        schema = dv.DataFrameSchema(name='fast_start_profiles', primary_keys=['unit'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='end_mode', data_type=np.int64, must_be_real_number=True,
                                          not_negative=True))
        schema.add_column(dv.SeriesSchema(name='time_in_end_mode', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.add_column(dv.SeriesSchema(name='mode_two_length', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.add_column(dv.SeriesSchema(name='mode_four_length', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.add_column(dv.SeriesSchema(name='min_loading', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.validate(fast_start_profiles)

    def set_demand_constraints(self, demand, violation_cost=None):
        """Creates constraints that force supply to equal to demand.

        Examples
        --------
        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create constraints.

        >>> market.set_demand_constraints(demand)

        The market should now have a set of constraints.

        >>> print(market._market_constraints_rhs_and_type['demand'])
          region  constraint_id type    rhs
        0    NSW              0    =  100.0

        ... and a mapping of those constraints to variable type for the lhs.

        >>> regional_mapping = market._constraint_to_variable_map['regional']

        >>> print(regional_mapping['demand'])
           constraint_id region service  coefficient
        0              0    NSW  energy          1.0

        Parameters
        ----------
        demand : pd.DataFrame
            Demand by region.

            ========  ================================================
            Columns:  Description:
            region    unique identifier of a region, (as `str`)
            demand    the non dispatchable demand, in MW, \n
                      (as `np.float64`)
            ========  ================================================

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

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
        if self.validate_inputs:
            self._validate_demand(demand)
        rhs_and_type, variable_map = market_constraints.energy(demand, self._next_constraint_id)
        self._market_constraints_rhs_and_type['demand'] = rhs_and_type
        self._constraint_to_variable_map['regional']['demand'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('demand', violation_cost)

    def _validate_demand(self, demand):
        schema = dv.DataFrameSchema(name='fast_start_profiles', primary_keys=['region'])
        schema.add_column(dv.SeriesSchema(name='region', data_type=str, allowed_values=self._market_regions))
        schema.add_column(dv.SeriesSchema(name='demand', data_type=np.float64, must_be_real_number=True))
        schema.validate(demand)

    def set_fcas_requirements_constraints(self, fcas_requirements, violation_cost=None):
        """Creates constraints that force FCAS supply to equal requirements.

        Examples
        --------
        Define the unit information data set needed to initialise the market, in this example all units are in the same
        region.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['QLD', 'NSW', 'VIC', 'SA'],
        ...                     unit_info=unit_info)

        Define a regulation raise FCAS requirement that apply to all mainland states.

        >>> fcas_requirements = pd.DataFrame({
        ...     'set': ['raise_reg_main', 'raise_reg_main',
        ...             'raise_reg_main', 'raise_reg_main'],
        ...     'service': ['raise_reg', 'raise_reg',
        ...                 'raise_reg', 'raise_reg'],
        ...     'region': ['QLD', 'NSW', 'VIC', 'SA'],
        ...     'volume': [100.0, 100.0, 100.0, 100.0]})

        Create constraints.

        >>> market.set_fcas_requirements_constraints(fcas_requirements)

        The market should now have a set of constraints.

        >>> print(market._market_constraints_rhs_and_type['fcas'])
                      set  constraint_id type    rhs
        0  raise_reg_main              0    =  100.0

        ... and a mapping of those constraints to variable type for the lhs.

        >>> regional_mapping = \
            market._constraint_to_variable_map['regional']

        >>> print(regional_mapping['fcas'])
           constraint_id    service region  coefficient
        0              0  raise_reg    QLD          1.0
        1              0  raise_reg    NSW          1.0
        2              0  raise_reg    VIC          1.0
        3              0  raise_reg     SA          1.0

        Parameters
        ----------
        fcas_requirements : pd.DataFrame
            requirement by set and the regions and service the requirement applies to.

            ========   ===============================================
            Columns:   Description:
            set        unique identifier of the requirement set, \n
                       (as `str`)
            service    the service or services the requirement set \n
                       applies to (as `str`)
            region     the regions that can contribute to meeting a \n
                       requirement, (as `str`)
            volume     the amount of service required, in MW, \n
                       (as `np.float64`)
            type       the direction of the constrain '=', '>=' or \n
                       '<=', optional, a value of '=' is assumed if \n
                       the column is missing (as `str`)
            ========   ===============================================

        violation_cost : float | pd.DataFrame
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any set, region and service combination.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the column 'set', 'service', 'region', or 'volume' is missing.
            UnexpectedColumn
                There is a column that is not 'set', 'service', 'region', 'volume' or 'type'.
            ColumnValues
                If there are inf, null or negative values in the volume column.
        """
        if self.validate_inputs:
            self._validate_fcas_requirements(fcas_requirements)
        rhs_and_type, variable_map = market_constraints.fcas(fcas_requirements, self._next_constraint_id)
        self._market_constraints_rhs_and_type['fcas'] = rhs_and_type
        self._constraint_to_variable_map['regional']['fcas'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('fcas', violation_cost)

    def _validate_fcas_requirements(self, fcas_requirements):
        schema = dv.DataFrameSchema(name='fcas_requirements', primary_keys=['set', 'region', 'service'])
        schema.add_column(dv.SeriesSchema(name='region', data_type=str, allowed_values=self._market_regions))
        schema.add_column(dv.SeriesSchema(name='set', data_type=str))
        schema.add_column(dv.SeriesSchema(name='service', data_type=str, allowed_values=self._allowed_services))
        schema.add_column(dv.SeriesSchema(name='volume', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='type', data_type=str, allowed_values=self._allowed_constraint_types),
                          optional=True)
        schema.validate(fcas_requirements)

    def set_fcas_max_availability(self, fcas_max_availability, violation_cost=None):
        """Creates constraints to ensure fcas dispatch is limited to the availability specified in the FCAS trapezium.

        The constraints are described in the
        :download:`FCAS MODEL IN NEMDE documentation section 2  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define the FCAS max_availability.

        >>> fcas_max_availability = pd.DataFrame({
        ... 'unit': ['A'],
        ... 'service': ['raise_6s'],
        ... 'max_availability': [60.0]})

        Set the joint availability constraints.

        >>> market.set_fcas_max_availability(fcas_max_availability)

        TNow the market should have the constraints and their mapping to decision varibales.

        >>> print(market._constraints_rhs_and_type['fcas_max_availability'])
          unit   service dispatch_type  constraint_id type   rhs
        0    A  raise_6s     generator              0   <=  60.0

        >>> unit_mapping = market._constraint_to_variable_map['unit_level']

        >>> print(unit_mapping['fcas_max_availability'])
           constraint_id unit   service dispatch_type  coefficient
        0              0    A  raise_6s     generator          1.0

        Parameters
        ----------
        fcas_max_availability : pd.DataFrame

            ================   =======================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit, \n
                               (as `str`)
            service            the fcas service being offered, \n
                               (as `str`)
            dispatch_type      "load" or "generator", optional default \n
                               (as `str`)
            max_availability   the maximum volume of the contingency \n
                               service, in MW, (as `np.float64`)
            ================   =======================================

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit and service combination.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the columns 'unit', 'service' or 'max_availability' is missing.
            UnexpectedColumn
                If there are columns other than 'unit', 'service' or 'max_availability'.
            ColumnValues
                If there are inf, null or negative values in the columns of type `np.float64`.
        """
        if self.validate_inputs:
            self._validate_fcas_max_availability(fcas_max_availability)
        rhs_and_type, variable_map = unit_constraints.fcas_max_availability(fcas_max_availability,
                                                                            self._next_constraint_id)
        self._constraints_rhs_and_type['fcas_max_availability'] = rhs_and_type
        self._constraint_to_variable_map['unit_level']['fcas_max_availability'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('fcas_max_availability', violation_cost)

    def _validate_fcas_max_availability(self, fcas_max_availability):
        schema = dv.DataFrameSchema(name='fcas_max_availability', primary_keys=['unit', 'service', 'dispatch_type'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='service', data_type=str, allowed_values=self._allowed_fcas_services))
        schema.add_column(dv.SeriesSchema(name='max_availability', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.add_column(dv.SeriesSchema(name='dispatch_type', data_type=str, allowed_values=['generator', 'load']),
                          optional=True)
        schema.validate(fcas_max_availability)

    def set_joint_ramping_constraints_reg(self, scada_ramp_rates, fast_start_profiles=None,
                                          run_type='no_fast_start_units', violation_cost=None):
        """Create constraints that ensure the provision of energy and fcas raise are within unit ramping capabilities.

        The constraints are described in the
        :download:`FCAS MODEL IN NEMDE documentation section 6.1  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        On a unit basis for generators they take the form of:

            Energy dispatch + Regulation raise target <= initial output + ramp up rate * (dispatch_interval / 60)

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info,
        ...                     dispatch_interval=60)

        Add bids to the market.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'service': ['raise_reg', 'raise_reg'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        >>> market.set_unit_volume_bids(volume_bids)

        Define unit initial outputs and ramping capabilities.

        >>> ramp_details = pd.DataFrame({
        ...   'unit': ['A', 'B'],
        ...   'initial_output': [100.0, 80.0],
        ...   'scada_ramp_up_rate': [20.0, 10.0],
        ...   'scada_ramp_down_rate': [20.0, 10.0]})

        Create the joint ramping constraints.

        >>> market.set_joint_ramping_constraints_reg(ramp_details)

        Now the market should have the constraints and their mapping to decision varibales.

        >>> print(market._constraints_rhs_and_type['joint_ramping_raise_reg'])
          unit  constraint_id type    rhs
        0    A              0   <=  120.0
        1    B              1   <=   90.0

        >>> unit_mapping = market._constraint_to_variable_map['unit_level']

        >>> print(unit_mapping['joint_ramping_raise_reg'])
           constraint_id unit    service dispatch_type  coefficient
        0              0    A  raise_reg     generator          1.0
        1              1    B  raise_reg     generator          1.0
        0              0    A     energy     generator          1.0
        1              1    B     energy     generator          1.0

        Parameters
        ----------

        scada_ramp_rates : pd.DataFrame

            Bidirectional units share scada ramp rates so only one set is given per unit and there is no need for the
            dispatch_type to be specified.

            ===================   ==========================================
            Columns:              Description:
            unit                  unique identifier of a dispatch unit, \n
                                  (as `str`)
            initial_output        the output of the unit at the start of \n
                                  the dispatch interval, in MW, \n
                                  (as `np.float64`)
            scada_ramp_up_rate    the scada maximum rate at which the unit can \n
                                  increase output, in MW/h, (as `np.float64`), \n
                                  optional
            scada_ramp_down_rate  the scada maximum rate at which the unit can \n
                                  decrease output, in MW/h, (as `np.float64`), \n
                                  optional.
            ====================  ==========================================


        fast_start_profiles : pd.DataFrame

            When run_type is set to 'no_fast_start_units': this table is not
                requried.

            When run_type is set to 'fast_start_first_run':

            ===================   ==========================================
            Columns:              Description:
            unit                  unique identifier of a dispatch unit, \n
                                  (as `str`)
            current_mode          The fast start operating mode of the unit \n
                                  at the start of the dispatch interval \n
                                  (as `int`)
            ====================  ==========================================

            When run_type is set to 'fast_start_first_run':

            ==========================   ==========================================
            Columns:                    Description:
            unit                        unique identifier of a dispatch unit, \n
                                        (as `str`)
            end_mode                    The fast start operating mode of the unit \n
                                        at the end of the dispatch interval \n
                                        (as `int`)
            time_since_end_of_mode_two  The number of minutes since the unit was \n
                                        operating in mode 2 in minutes, can be nan \n
                                        if the unit (as `int`)
            min_loading                 the fast start profile min loading in MW \n
                                        (as `foat`)
            ==========================  ==========================================

        run_type: str specifying whever this the fist fast start run or the second, or
            if fast start units are not being considered. One of 'no_fast_start_units',
            'fast_start_first_run', or 'fast_start_second_run'.

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

        Returns
        -------
        None

        Raises
        ------
            RepeatedRowError
                If there is more than one row for any unit in unit_limits.
            ColumnDataTypeError
                If columns are not of the required type.
            MissingColumnError
                If the columns 'unit', 'initial_output' or 'ramp_up_rate' are missing from unit_limits.
            UnexpectedColumn
                If there are columns other than 'unit', 'initial_output' or 'ramp_up_rate' in unit_limits.
            ColumnValues
                If there are inf, null or negative values in the columns of type `np.float64`.
        """
        if self.validate_inputs:
            self._validate_scada_ramp_rates_for_reg(scada_ramp_rates)

        if run_type != "no_fast_start_units" and fast_start_profiles is not None and self.validate_inputs:
            self._validate_fast_start_profiles_for_ramp_rates(fast_start_profiles, run_type)

        bid_variables = self._decision_variables['bids']

        raise_reg_units = \
            bid_variables[bid_variables['service'] == 'raise_reg'].loc[:,['unit', 'dispatch_type', 'service']]
        raise_reg_units = raise_reg_units.drop_duplicates()
        lower_reg_units = \
            bid_variables[bid_variables['service'] == 'lower_reg'].loc[:,['unit', 'dispatch_type', 'service']]
        lower_reg_units = lower_reg_units.drop_duplicates()

        ramp_rates = scada_ramp_rates.rename(
            columns={'scada_ramp_up_rate': 'ramp_up_rate',
                     'scada_ramp_down_rate': 'ramp_down_rate'}
        )

        ramp_rates = rrp._adjust_ramp_rates_for_fast_start_profiles(
            ramp_rates, run_type, fast_start_profiles, self.dispatch_interval
        )

        ramp_up_rates_for_raise_reg = pd.merge(
            raise_reg_units,
            ramp_rates,
            on='unit'
        )

        ramp_up_rates_for_raise_reg = \
            ramp_up_rates_for_raise_reg.loc[:, ['unit', 'dispatch_type', 'service', 'ramp_up_rate', 'initial_output']]
        ramp_up_rates_for_raise_reg = ramp_up_rates_for_raise_reg.rename(columns={'ramp_up_rate': 'ramp_rate'})

        ramp_up_rates_for_raise_reg_bdu = ramp_up_rates_for_raise_reg[
            ramp_up_rates_for_raise_reg['unit'].isin(self._bidirectional_units)]
        ramp_up_rates_for_raise_reg = ramp_up_rates_for_raise_reg[
            ~ramp_up_rates_for_raise_reg['unit'].isin(self._bidirectional_units)]

        if not ramp_up_rates_for_raise_reg.empty:
            rhs_and_type, variable_map = \
                fcas_constraints.joint_ramping_constraints_raise_reg(
                    ramp_up_rates_for_raise_reg, self.dispatch_interval, self._next_constraint_id)
            self._constraints_rhs_and_type['joint_ramping_raise_reg'] = rhs_and_type
            self._constraint_to_variable_map['unit_level']['joint_ramping_raise_reg'] = variable_map
            self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if not ramp_up_rates_for_raise_reg_bdu.empty:
            ramp_up_rates_for_raise_reg_bdu = \
                ramp_up_rates_for_raise_reg_bdu[~ramp_up_rates_for_raise_reg_bdu.isna()]
            rhs_and_type, variable_map = \
                fcas_constraints.joint_ramping_constraints_raise_reg_bdu(
                    ramp_up_rates_for_raise_reg_bdu, self.dispatch_interval, self._next_constraint_id)
            self._constraints_rhs_and_type['joint_ramping_raise_reg_bdu'] = rhs_and_type
            self._constraint_to_variable_map['unit_level']['joint_ramping_raise_reg_bdu'] = variable_map
            self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        ramp_down_rates_for_lower_reg = pd.merge(
            lower_reg_units,
            ramp_rates,
            on='unit'
        )

        ramp_down_rates_for_lower_reg = \
            ramp_down_rates_for_lower_reg.loc[:, ['unit', 'dispatch_type', 'service', 'ramp_down_rate', 'initial_output']]
        ramp_down_rates_for_lower_reg = ramp_down_rates_for_lower_reg.rename(columns={'ramp_down_rate': 'ramp_rate'})

        ramp_down_rates_for_lower_reg_bdu = ramp_down_rates_for_lower_reg[
            ramp_down_rates_for_lower_reg['unit'].isin(self._bidirectional_units)]
        ramp_down_rates_for_lower_reg = ramp_down_rates_for_lower_reg[
            ~ramp_down_rates_for_lower_reg['unit'].isin(self._bidirectional_units)]

        if not ramp_down_rates_for_lower_reg.empty:
            rhs_and_type, variable_map = \
                fcas_constraints.joint_ramping_constraints_lower_reg(
                    ramp_down_rates_for_lower_reg, self.dispatch_interval, self._next_constraint_id)
            self._constraints_rhs_and_type['joint_ramping_lower_reg'] = rhs_and_type
            self._constraint_to_variable_map['unit_level']['joint_ramping_lower_reg'] = variable_map
            self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if not ramp_down_rates_for_lower_reg_bdu.empty:
            ramp_down_rates_for_lower_reg_bdu = \
                ramp_down_rates_for_lower_reg_bdu[~ramp_down_rates_for_lower_reg_bdu.isna()]
            rhs_and_type, variable_map = \
                fcas_constraints.joint_ramping_constraints_lower_reg_bdu(
                    ramp_down_rates_for_lower_reg_bdu, self.dispatch_interval, self._next_constraint_id)
            self._constraints_rhs_and_type['joint_ramping_lower_reg_bdu'] = rhs_and_type
            self._constraint_to_variable_map['unit_level']['joint_ramping_lower_reg_bdu'] = variable_map
            self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('joint_ramping_raise_reg', violation_cost)
            self.make_constraints_elastic('joint_ramping_lower_reg', violation_cost)
            if 'joint_ramping_raise_reg_bdu' in self._constraint_to_variable_map['unit_level'].keys():
                self.make_constraints_elastic('joint_ramping_raise_reg_bdu', violation_cost)
            if 'joint_ramping_lower_reg_bdu' in self._constraint_to_variable_map['unit_level'].keys():
                self.make_constraints_elastic('joint_ramping_lower_reg_bdu', violation_cost)

    def _validate_scada_ramp_rates_for_reg(self, ramp_details):
        schema = dv.DataFrameSchema(name='ramp_details', primary_keys=['unit'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='initial_output', data_type=np.float64))
        schema.add_column(dv.SeriesSchema(name='scada_ramp_up_rate', data_type=np.float64))
        schema.add_column(dv.SeriesSchema(name='scada_ramp_down_rate', data_type=np.float64))
        schema.validate(ramp_details)

    def set_joint_capacity_constraints(self, contingency_trapeziums, violation_cost=None):
        """Creates constraints to ensure there is adequate capacity for contingency, regulation and energy dispatch.

        Create two constraints for each contingency services, one ensures operation on upper slope of the fcas
        contingency trapezium is consistent with regulation raise and energy dispatch, the second ensures operation on
        upper slope of the fcas contingency trapezium is consistent with regulation lower and energy dispatch.

        The constraints are described in the
        :download:`FCAS MODEL IN NEMDE documentation section 6.2  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A'],
        ...     'region': ['NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

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

        >>> market.set_joint_capacity_constraints(contingency_trapeziums)

        TNow the market should have the constraints and their mapping to decision varibales.

        >>> print(market._constraints_rhs_and_type['joint_capacity'])
          unit   service dispatch_type  constraint_id type   rhs
        0    A  raise_6s     generator              0   <=  80.0
        0    A  raise_6s     generator              1   >=  20.0

        >>> unit_mapping = market._constraint_to_variable_map['unit_level']

        >>> print(unit_mapping['joint_capacity'])
           constraint_id unit dispatch_type    service  coefficient
        0              0    A     generator     energy     1.000000
        0              0    A     generator   raise_6s     0.333333
        0              0    A     generator  raise_reg     1.000000
        0              1    A     generator     energy     1.000000
        0              1    A     generator   raise_6s    -0.333333
        0              1    A     generator  lower_reg    -1.000000

        Parameters
        ----------
        contingency_trapeziums : pd.DataFrame

            ================   =======================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit, \n
                               (as `str`)
            service            the contingency service being offered, \n
                               (as `str`)
            dispatch_type      "load" or "generator", optional default \n
                               (as `str`)
            max_availability   the maximum volume of the contingency \n
                               service, in MW, (as `np.float64`)
            enablement_min     the energy dispatch level at which the \n
                               unit can begin to provide the, \n
                               contingency service, in MW, \n
                               (as `np.float64`)
            low_break_point    the energy dispatch level at which \n
                               the unit can provide the full \n
                               contingency service offered, in MW, \n
                               (as `np.float64`)
            high_break_point   the energy dispatch level at which \n
                               the unit can no longer provide the \n
                               full contingency service offered, \n
                               in MW, (as `np.float64`)
            enablement_max     the energy dispatch level at which \n
                               the unit can no longer provide \n
                               the contingency service, in MW, \n
                               (as `np.float64`)
            ================   =======================================

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

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
        if self.validate_inputs:
            self._validate_contingency_trapeziums(contingency_trapeziums)
        rhs_and_type, variable_map = fcas_constraints.joint_capacity_constraints(
            contingency_trapeziums, self._bidirectional_units, self._next_constraint_id)
        self._constraints_rhs_and_type['joint_capacity'] = rhs_and_type
        self._constraint_to_variable_map['unit_level']['joint_capacity'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('joint_capacity', violation_cost)

    def _validate_contingency_trapeziums(self, contingency_trapeziums):
        schema = dv.DataFrameSchema(name='contingency_trapeziums', primary_keys=['unit', 'service', 'dispatch_type'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='service', data_type=str,
                                          allowed_values=self._allowed_contingency_fcas_services))
        schema.add_column(dv.SeriesSchema(name='max_availability', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.add_column(dv.SeriesSchema(name='enablement_min', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='low_break_point', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='high_break_point', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='enablement_max', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='dispatch_type', data_type=str, allowed_values=['generator', 'load']),
                          optional=True)
        schema.validate(contingency_trapeziums)

    def set_energy_and_regulation_capacity_constraints(self, regulation_trapeziums, violation_cost=None):
        """Creates constraints to ensure there is adequate capacity for regulation and energy dispatch targets.

        Create two constraints for each regulation services, one ensures operation on upper slope of the fcas
        regulation trapezium is consistent with energy dispatch, the second ensures operation on lower slope of the
        fcas regulation trapezium is consistent with energy dispatch.

        The constraints are described in the
        :download:`FCAS MODEL IN NEMDE documentation section 6.3  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A'],
        ...     'region': ['NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

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

        >>> market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)

        TNow the market should have the constraints and their mapping to decision varibales.

        >>> print(market._constraints_rhs_and_type['energy_and_regulation_capacity'])
          unit    service dispatch_type  constraint_id type   rhs
        0    A  raise_reg     generator              0   <=  80.0
        0    A  raise_reg     generator              1   >=  20.0


        >>> unit_mapping = market._constraint_to_variable_map['unit_level']

        >>> print(unit_mapping['energy_and_regulation_capacity'])
           constraint_id unit dispatch_type    service  coefficient
        0              0    A     generator     energy     1.000000
        0              0    A     generator  raise_reg     0.333333
        0              1    A     generator     energy     1.000000
        0              1    A     generator  raise_reg    -0.333333

        Parameters
        ----------
        regulation_trapeziums : pd.DataFrame
            The FCAS trapeziums for the regulation services being offered.

            ================   =======================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit, \n
                               (as `str`)
            service            the regulation service being offered, \n
                               (as `str`)
            dispatch_type      "load" or "generator", optional default \n
                               (as `str`)
            max_availability   the maximum volume of the contingency \n
                               service, in MW, (as `np.float64`)
            enablement_min     the energy dispatch level at which \n
                               the unit can begin to provide \n
                               the regulation service, in MW, \n
                               (as `np.float64`)
            low_break_point    the energy dispatch level at which \n
                               the unit can provide the full \n
                               regulation service offered, in MW, \n
                               (as `np.float64`)
            high_break_point   the energy dispatch level at which the \n
                               unit can no longer provide the \n
                               full regulation service offered, in MW, \n
                               (as `np.float64`)
            enablement_max     the energy dispatch level at which the \n
                               unit can no longer provide any \n
                               regulation service, in MW, \n
                               (as `np.float64`)
            ================   =======================================

        violation_cost : float
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

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
        if variable_ids:
            self._validate_regulation_trapeziums(regulation_trapeziums)
        rhs_and_type, variable_map = \
            fcas_constraints.energy_and_regulation_capacity_constraints(
                regulation_trapeziums, self._next_constraint_id

            )
        self._constraints_rhs_and_type['energy_and_regulation_capacity'] = rhs_and_type
        self._constraint_to_variable_map['unit_level']['energy_and_regulation_capacity'] = variable_map
        self._next_constraint_id = max(rhs_and_type['constraint_id']) + 1

        if violation_cost is not None:
            self.make_constraints_elastic('energy_and_regulation_capacity', violation_cost)

    def _validate_regulation_trapeziums(self, contingency_trapeziums):
        schema = dv.DataFrameSchema(name='contingency_trapeziums', primary_keys=['unit', 'service', 'dispatch_type'])
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=self._unit_info['unit']))
        schema.add_column(dv.SeriesSchema(name='service', data_type=str,
                                          allowed_values=self._allowed_regulation_fcas_services))
        schema.add_column(dv.SeriesSchema(name='max_availability', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.add_column(dv.SeriesSchema(name='enablement_min', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='low_break_point', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='high_break_point', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='enablement_max', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='dispatch_type', data_type=str, allowed_values=['generator', 'load']),
                          optional=True)
        schema.validate(contingency_trapeziums)

    def set_interconnectors(self, interconnector_directions_and_limits):
        """Create lossless links between specified regions.

        Examples
        --------

        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A'],
        ...     'region': ['NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW', 'VIC'],
        ...                     unit_info=unit_info)

        Define a an interconnector between NSW and VIC so generator can A can be used to meet demand in VIC.

        >>> interconnector = pd.DataFrame({
        ...     'interconnector': ['inter_one'],
        ...     'to_region': ['VIC'],
        ...     'from_region': ['NSW'],
        ...     'max': [100.0],
        ...     'min': [-100.0]})

        Create the interconnector.

        >>> market.set_interconnectors(interconnector)

        The market should now have a decision variable defined for each interconnector.

        >>> print(market._decision_variables['interconnectors'])
          interconnector       link  variable_id  lower_bound  upper_bound        type  generic_constraint_factor
        0      inter_one  inter_one            0       -100.0        100.0  continuous                          1

        ... and a mapping of those variables to to regional energy constraints.

        >>> regional = market._variable_to_constraint_map['regional']

        >>> print(regional['interconnectors'])
           variable_id interconnector       link region service  coefficient
        0            0      inter_one  inter_one    VIC  energy          1.0
        1            0      inter_one  inter_one    NSW  energy         -1.0

        Parameters
        ----------
        interconnector_directions_and_limits : pd.DataFrame

            ========================  ================================
            Columns:                  Description:
            interconnector            unique identifier of a  \n
                                      interconnector, (as `str`)
            to_region                 the region that receives power \n
                                      when flow is in the positive \n
                                      direction, (as `str`)
            from_region               the region that power is drawn \n
                                      from when flow is in the \n
                                      positive direction, (as `str`)
            max                       the maximum power flow in the \n
                                      positive direction, in MW, \n
                                      (as `np.float64`)
            min                       the maximum power flow in the \n
                                      negative direction, in MW, \n
                                      (as `np.float64`)
            from_region_loss_factor   the loss factor at the from \n
                                      region end of the interconnector, \n
                                      refers the the from region end \n
                                      to the regional reference node, \n
                                      optional, assumed to equal 1.0, \n
                                      if the column is not provided, \n
                                      (as `np.float`)
            to_region_loss_factor     the loss factor at the to region \n
                                      end of the interconnector, \n
                                      refers the to region end to the \n
                                      regional reference node, \n
                                      optional, assumed equal to 1.0 \n
                                      if the column is not provided, \n
                                      (as `np.float`)
            ========================  ================================

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
        if 'link' not in interconnector_directions_and_limits.columns:
            interconnector_directions_and_limits['link'] = interconnector_directions_and_limits['interconnector']
        if 'from_region_loss_factor' not in interconnector_directions_and_limits.columns:
            interconnector_directions_and_limits['from_region_loss_factor'] = 1.0
        if 'to_region_loss_factor' not in interconnector_directions_and_limits.columns:
            interconnector_directions_and_limits['to_region_loss_factor'] = 1.0
        if 'generic_constraint_factor' not in interconnector_directions_and_limits.columns:
            interconnector_directions_and_limits['generic_constraint_factor'] = 1

        if self.validate_inputs:
            self._validate_interconnector_definitions(interconnector_directions_and_limits)

        self._interconnector_directions = interconnector_directions_and_limits

        self._decision_variables['interconnectors'], self._variable_to_constraint_map['regional']['interconnectors'] \
            = inter.create(interconnector_directions_and_limits, self._next_variable_id)

        self._next_variable_id = max(self._decision_variables['interconnectors']['variable_id']) + 1

    def _validate_interconnector_definitions(self, interconnector_directions_and_limits):
        schema = dv.DataFrameSchema(name='interconnector_directions_and_limits',
                                    primary_keys=['interconnector', 'link'])
        schema.add_column(dv.SeriesSchema(name='interconnector', data_type=str))
        schema.add_column(dv.SeriesSchema(name='link', data_type=str))
        schema.add_column(dv.SeriesSchema(name='to_region', data_type=str, allowed_values=self._market_regions))
        schema.add_column(dv.SeriesSchema(name='from_region', data_type=str, allowed_values=self._market_regions))
        schema.add_column(dv.SeriesSchema(name='max', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='min', data_type=np.float64, must_be_real_number=True))
        schema.add_column(dv.SeriesSchema(name='generic_constraint_factor', data_type=np.int64, allowed_values=[1, -1]))
        schema.add_column(dv.SeriesSchema(name='from_region_loss_factor', data_type=np.float64,
                                          must_be_real_number=True, not_negative=True))
        schema.add_column(dv.SeriesSchema(name='to_region_loss_factor', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.validate(interconnector_directions_and_limits)

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

        Constrain the weight variables to give the relative weighting of adjacent breakpoint:

            w1 * -100.0 + w2 * 0.0 + w3 * 100.0 = interconnector flow

        Constrain the interconnector losses to be the weighted sum of the losses at the adjacent break point:

            w1 * f(-100.0) + w2 * f(0.0) + w3 * f(100.0) = interconnector losses

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A'],
        ...     'region': ['NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW', 'VIC'],
        ...                     unit_info=unit_info)

        Create the interconnector, this need to be done before a interconnector losses can be set.

        >>> interconnectors = pd.DataFrame({
        ...    'interconnector': ['little_link'],
        ...    'to_region': ['VIC'],
        ...    'from_region': ['NSW'],
        ...    'max': [100.0],
        ...    'min': [-120.0]})

        >>> market.set_interconnectors(interconnectors)

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
        ...    'loss_segment': [1, 2, 3],
        ...    'break_point': [-120.0, 0.0, 100]})

        >>> market.set_interconnector_losses(loss_functions, interpolation_break_points)

        The market should now have a decision variable defined for each interconnector's losses.

        >>> print(market._decision_variables['interconnector_losses'])
          interconnector         link  variable_id  lower_bound  upper_bound        type
        0    little_link  little_link            1       -120.0        120.0  continuous

        ... and a mapping of those variables to regional energy constraints.

        >>> print(market._variable_to_constraint_map['regional']['interconnector_losses'])
           variable_id region service  coefficient
        0            1    VIC  energy         -0.5
        1            1    NSW  energy         -0.5

        The market will also have a special ordered set of weight variables for interpolating the loss function
        between the break points.

        >>> print(market._decision_variables['interpolation_weights'].loc[:,
        ...       ['interconnector', 'loss_segment', 'break_point', 'variable_id']])
          interconnector  loss_segment  break_point  variable_id
        0    little_link             1       -120.0            2
        1    little_link             2          0.0            3
        2    little_link             3        100.0            4

        >>> print(market._decision_variables['interpolation_weights'].loc[:,
        ...       ['variable_id', 'lower_bound', 'upper_bound', 'type']])
           variable_id  lower_bound  upper_bound        type
        0            2          0.0          1.0  continuous
        1            3          0.0          1.0  continuous
        2            4          0.0          1.0  continuous

        and a set of constraints that implement the interpolation, see above explanation.

        >>> print(market._constraints_rhs_and_type['interpolation_weights'])
          interconnector         link  constraint_id type  rhs
        0    little_link  little_link              0    =  1.0

        >>> print(market._constraints_dynamic_rhs_and_type['link_loss_to_flow'])
          interconnector         link  constraint_id type  rhs_variable_id
        0    little_link  little_link              2    =                0
        0    little_link  little_link              1    =                1

        >>> print(market._lhs_coefficients['interconnector_losses'])
           variable_id  constraint_id  coefficient
        0            2              0          1.0
        1            3              0          1.0
        2            4              0          1.0
        0            2              2       -120.0
        1            3              2          0.0
        2            4              2        100.0
        0            2              1          6.0
        1            3              1          0.0
        2            4              1          5.0


        Parameters
        ----------
        loss_functions : pd.DataFrame

            ======================  ==================================
            Columns:                Description:
            interconnector          unique identifier of a \n
                                    interconnector, (as `str`)
            from_region_loss_share  The fraction of loss occuring in \n
                                    the from region, 0.0 to 1.0, \n
                                    (as `np.float64`)
            loss_function           A function that takes a flow, \n
                                    in MW as a float and returns the \n
                                    losses in MW, (as `callable`)
            ======================  ==================================

        interpolation_break_points : pd.DataFrame

            ==============  ==========================================
            Columns:        Description:
            interconnector  unique identifier of a interconnector, \n
                            (as `str`)
            loss_segment    unique identifier of a loss segment on \n
                            an interconnector basis, (as `np.float64`)
            break_point     points between which the loss function \n
                            will be linearly interpolated, in MW, \n
                            (as `np.float64`)
            ==============  ==========================================

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
        if 'interconnectors' not in self._decision_variables:
            ModelBuildError('Interconnector losses cannot be set before interconnectors have been added to the model.')

        if 'link' not in loss_functions.columns:
            loss_functions['link'] = loss_functions['interconnector']

        if 'link' not in interpolation_break_points.columns:
            interpolation_break_points['link'] = interpolation_break_points['interconnector']

        if self.validate_inputs:
            self._validate_loss_functions(loss_functions)
            self._validate_interpolation_break_points(interpolation_break_points)

        self._interconnector_loss_shares = loss_functions.loc[:, ['interconnector', 'link', 'from_region_loss_share']]

        loss_functions = pd.merge(loss_functions,
                                  self._interconnector_directions.loc[:, ['interconnector', 'link', 'from_region']],
                                  on=['interconnector', 'link'])

        loss_variables, loss_variables_constraint_map = \
            inter.create_loss_variables(self._decision_variables['interconnectors'],
                                        self._variable_to_constraint_map['regional']['interconnectors'],
                                        loss_functions,
                                        self._next_variable_id)

        next_variable_id = loss_variables['variable_id'].max() + 1

        weight_variables = inter.create_weights(interpolation_break_points, next_variable_id)

        # Creates weights sum constraint.
        weights_sum_lhs, weights_sum_rhs = inter.create_weights_must_sum_to_one(weight_variables,
                                                                                self._next_constraint_id)
        next_constraint_id = weights_sum_rhs['constraint_id'].max() + 1

        # Link the losses to the interpolation weights.
        link_to_loss_lhs, link_to_loss_rhs = \
            inter.link_inter_loss_to_interpolation_weights(weight_variables, loss_variables, loss_functions,
                                                           next_constraint_id)
        next_constraint_id = link_to_loss_rhs['constraint_id'].max() + 1

        # Link weights to interconnector flow.
        link_to_flow_lhs, link_to_flow_rhs = inter.link_weights_to_inter_flow(weight_variables,
                                                                              self._decision_variables[
                                                                                  'interconnectors'],
                                                                              next_constraint_id)

        # Combine lhs sides, note these are complete lhs and don't need to be mapped to constraints.
        lhs = pd.concat([weights_sum_lhs, link_to_flow_lhs, link_to_loss_lhs])

        # Combine constraints with a dynamic rhs i.e. a variable on the rhs.
        dynamic_rhs = pd.concat([link_to_flow_rhs, link_to_loss_rhs])

        # Save results.
        self._decision_variables['interconnector_losses'] = loss_variables
        self._variable_to_constraint_map['regional']['interconnector_losses'] = loss_variables_constraint_map
        self._decision_variables['interpolation_weights'] = weight_variables
        self._lhs_coefficients['interconnector_losses'] = lhs
        self._constraints_rhs_and_type['interpolation_weights'] = weights_sum_rhs
        self._constraints_dynamic_rhs_and_type['link_loss_to_flow'] = dynamic_rhs
        self._next_variable_id = pd.concat([loss_variables, weight_variables])['variable_id'].max() + 1
        self._next_constraint_id = pd.concat([weights_sum_rhs, dynamic_rhs])['constraint_id'].max() + 1

    @staticmethod
    def _validate_loss_functions(loss_functions):
        schema = dv.DataFrameSchema(name='loss_functions', primary_keys=['interconnector', 'link'])
        schema.add_column(dv.SeriesSchema(name='interconnector', data_type=str))
        schema.add_column(dv.SeriesSchema(name='link', data_type=str))
        schema.add_column(dv.SeriesSchema(name='loss_function', data_type=callable))
        schema.add_column(dv.SeriesSchema(name='from_region_loss_share', data_type=np.float64, must_be_real_number=True,
                                          not_negative=True))
        schema.validate(loss_functions)

    @staticmethod
    def _validate_interpolation_break_points(interpolation_break_points):
        schema = dv.DataFrameSchema(name='interpolation_break_points', primary_keys=['interconnector', 'link',
                                                                                     'loss_segment'])
        schema.add_column(dv.SeriesSchema(name='interconnector', data_type=str))
        schema.add_column(dv.SeriesSchema(name='link', data_type=str))
        schema.add_column(dv.SeriesSchema(name='loss_segment', data_type=np.int64))
        schema.add_column(dv.SeriesSchema(name='break_point', data_type=np.float64, must_be_real_number=True))
        schema.validate(interpolation_break_points)

    def set_generic_constraints(self, generic_constraint_parameters, violation_cost=None):
        """Creates a set of generic constraints, adding the constraint type, rhs.

        This sets a set of arbitrary constraints, but only the type and rhs values. The lhs terms can be added to these
        constraints using the methods link_units_to_generic_constraints, link_interconnectors_to_generic_constraints
        and link_regions_to_generic_constraints.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A'],
        ...     'region': ['NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define a set of generic constraints and add them to the market.

        >>> generic_constraint_parameters = pd.DataFrame({
        ...   'set': ['A', 'B'],
        ...   'type': ['>=', '<='],
        ...   'rhs': [10.0, -100.0]})

        >>> market.set_generic_constraints(generic_constraint_parameters)

        Now the market should have a set of generic constraints.

        >>> print(market._constraints_rhs_and_type['generic'])
          set  constraint_id type    rhs
        0   A              0   >=   10.0
        1   B              1   <= -100.0

        Parameters
        ----------
        generic_constraint_parameters : pd.DataFrame

            =============  ===========================================
            Columns:       Description:
            set            the unique identifier of the constraint set, \n
                           (as `str`)
            type           the direction of the constraint >=, <= or \n
                           =, (as `str`)
            rhs            the right hand side value of the constraint, \n
                           (as `np.float64`)
            =============  ===========================================

        violation_cost : float | pd.DataFrame
            Makes assocaited constrainst elastic using the given violation_cost (in $/MW).

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
                If the column 'set', 'type' or 'rhs' is missing.
            UnexpectedColumn
                There is a column that is not 'set', 'type' or 'rhs' .
            ColumnValues
                If there are inf or null values in the rhs column.
        """
        if self.validate_inputs:
            self._validate_generic_constraint_parameters(generic_constraint_parameters)
        type_and_rhs = hf.save_index(generic_constraint_parameters, 'constraint_id', self._next_constraint_id)
        self._constraints_rhs_and_type['generic'] = type_and_rhs.loc[:, ['set', 'constraint_id', 'type', 'rhs']]
        self._next_constraint_id = type_and_rhs['constraint_id'].max() + 1

        if violation_cost is not None:
            self.make_constraints_elastic('generic', violation_cost)

    @staticmethod
    def _validate_generic_constraint_parameters(generic_constraint_parameters):
        schema = dv.DataFrameSchema(name='generic_constraint_parameters', primary_keys=['set'])
        schema.add_column(dv.SeriesSchema(name='set', data_type=str))
        schema.add_column(dv.SeriesSchema(name='type', data_type=str))
        schema.add_column(dv.SeriesSchema(name='rhs', data_type=np.float64, must_be_real_number=True))
        schema.validate(generic_constraint_parameters)

    def link_units_to_generic_constraints(self, unit_coefficients):
        """Set the lhs coefficients of generic constraints on unit basis.

        Notes
        -----
        These sets also maps to the sets in the fcas market constraints. One potential use of this is prevent specific
        units from helping to meet fcas constraints by giving them a negative one (-1.0) coefficient using this method
        for particular fcas markey constraints.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'X', 'Y'],
        ...     'region': ['NSW', 'NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW', 'VIC'],
        ...                     unit_info=unit_info)

        Define unit lhs coefficients for generic constraints.

        >>> unit_coefficients = pd.DataFrame({
        ...   'set': ['A', 'A', 'B'],
        ...   'unit': ['X', 'Y', 'X'],
        ...   'service': ['energy', 'energy', 'raise_reg'],
        ...   'coefficient': [1.0, 1.0, -1.0]})

        >>> market.link_units_to_generic_constraints(unit_coefficients)

        Note all this does is save this information to the market object, linking to specific variable ids and
        constraint id occurs when the dispatch method is called.

        >>> print(market._generic_constraint_lhs['unit'])
          set unit    service  coefficient
        0   A    X     energy          1.0
        1   A    Y     energy          1.0
        2   B    X  raise_reg         -1.0

        Parameters
        ----------
        unit_coefficients : pd.DataFrame

            =============  ===========================================
            Columns:       Description:
            set            the unique identifier of the constraint set \n
                           to map the lhs coefficients to, (as `str`)
            unit           the unit whose variables will be mapped to \n
                           the lhs, (as `str`)
            service        the service whose variables will be mapped
                           to the lhs, (as `str`)
            coefficient    the lhs coefficient (as `np.float64`)
            =============  ===========================================

        Raises
        ------
        RepeatedRowError
            If there is more than one row for any set, unit and service combination.
        ColumnDataTypeError
            If columns are not of the required type.
        MissingColumnError
            If the column 'set', 'unit', 'serice' or 'coefficient' is missing.
        UnexpectedColumn
            There is a column that is not 'set', 'unit', 'serice' or 'coefficient'.
        ColumnValues
            If there are inf or null values in the rhs coefficient.
        """
        if self.validate_inputs:
            self._validate_generic_unit_coefficients(unit_coefficients)
        self._generic_constraint_lhs['unit'] = unit_coefficients

    def _validate_generic_unit_coefficients(self, unit_coefficients):
        schema = dv.DataFrameSchema(name='unit_coefficients', primary_keys=['set', 'unit', 'service'])
        schema.add_column(dv.SeriesSchema(name='set', data_type=str))
        schema.add_column(dv.SeriesSchema(name='unit', data_type=str, allowed_values=list(self._unit_info['unit'])))
        schema.add_column(dv.SeriesSchema(name='service', data_type=str, allowed_values=self._allowed_services))
        schema.add_column(dv.SeriesSchema(name='coefficient', data_type=np.float64, must_be_real_number=True))
        schema.validate(unit_coefficients)

    def link_regions_to_generic_constraints(self, region_coefficients):
        """Set the lhs coefficients of generic constraints on region basis.

        This effectively acts as short cut for mapping unit variables to a generic constraint. If a particular
        service in a particular region is included here then all units in this region will have their variables
        of this service included on the lhs of this constraint set. If a particular unit needs to be excluded
        from an otherwise region wide constraint it can be given a coefficient with opposite sign to the region
        wide sign in the generic unit constraints, the coefficients from the two lhs set will be summed and cancel
        each other out.

        Notes
        -----
        These sets also map to the sets in the fcas market constraints.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['X', 'X']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['X', 'Y'],
        ...                     unit_info=unit_info)

        Define region lhs coefficients for generic constraints.

        >>> region_coefficients = pd.DataFrame({
        ...   'set': ['A', 'A', 'B'],
        ...   'region': ['X', 'Y', 'X'],
        ...   'service': ['energy', 'energy', 'raise_reg'],
        ...   'coefficient': [1.0, 1.0, -1.0]})

        >>> market.link_regions_to_generic_constraints(region_coefficients)

        Note all this does is save this information to the market object, linking to specific variable ids and
        constraint id occurs when the dispatch method is called.

        >>> print(market._generic_constraint_lhs['region'])
          set region    service  coefficient
        0   A      X     energy          1.0
        1   A      Y     energy          1.0
        2   B      X  raise_reg         -1.0

        Parameters
        ----------
        region_coefficients : pd.DataFrame

            =============  ===========================================
            Columns:       Description:
            set            the unique identifier of the constraint set \n
                           to map the lhs coefficients to, (as `str`)
            region         the region whose variables will be mapped \n
                           to the lhs, (as `str`)
            service        the service whose variables will be mapped \n
                           to the lhs, (as `str`)
            coefficient    the lhs coefficient (as `np.float64`)
            =============  ===========================================

        Raises
        ------
        RepeatedRowError
            If there is more than one row for any set, region and service combination.
        ColumnDataTypeError
            If columns are not of the required type.
        MissingColumnError
            If the column 'set', 'region', 'service' or 'coefficient' is missing.
        UnexpectedColumn
            There is a column that is not 'set', 'region', 'service' or 'coefficient'.
        ColumnValues
            If there are inf or null values in the rhs coefficient.
        """
        if self.validate_inputs:
            self._validate_generic_region_coefficients(region_coefficients)
        self._generic_constraint_lhs['region'] = region_coefficients

    def _validate_generic_region_coefficients(self, region_coefficients):
        schema = dv.DataFrameSchema(name='region_coefficients', primary_keys=['set', 'region', 'service'])
        schema.add_column(dv.SeriesSchema(name='set', data_type=str))
        schema.add_column(dv.SeriesSchema(name='region', data_type=str, allowed_values=self._market_regions))
        schema.add_column(dv.SeriesSchema(name='service', data_type=str, allowed_values=self._allowed_services))
        schema.add_column(dv.SeriesSchema(name='coefficient', data_type=np.float64, must_be_real_number=True))
        schema.validate(region_coefficients)

    def link_interconnectors_to_generic_constraints(self, interconnector_coefficients):
        """Set the lhs coefficients of generic constraints on an interconnector basis.

        Notes
        -----
        These sets also map to the set in the fcas market constraints.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['C', 'D'],
        ...     'region': ['X', 'X']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['X', 'Y'],
        ...                     unit_info=unit_info)

        Define region lhs coefficients for generic constraints. All interconnector variables are for the energy service
        so no 'service' can be specified.

        >>> interconnector_coefficients = pd.DataFrame({
        ...   'set': ['A', 'A', 'B'],
        ...   'interconnector': ['X', 'Y', 'X'],
        ...   'coefficient': [1.0, 1.0, -1.0]})

        >>> market.link_interconnectors_to_generic_constraints(interconnector_coefficients)

        Note all this does is save this information to the market object, linking to specific variable ids and
        constraint id occurs when the dispatch method is called.

        >>> print(market._generic_constraint_lhs['interconnectors'])
          set interconnector  coefficient
        0   A              X          1.0
        1   A              Y          1.0
        2   B              X         -1.0

        Parameters
        ----------
        unit_coefficients : pd.DataFrame

            =============   ==========================================
            Columns:        Description:
            set             the unique identifier of the constraint set \n
                            to map the lhs coefficients to, (as `str`)
            interconnetor   the interconnetor whose variables will be \n
                            mapped to the lhs, (as `str`)
            coefficient     the lhs coefficient (as `np.float64`)
            =============   ==========================================

        Raises
        ------
        RepeatedRowError
            If there is more than one row for any set, interconnetor and service combination.
        ColumnDataTypeError
            If columns are not of the required type.
        MissingColumnError
            If the column 'set', 'interconnetor' or 'coefficient' is missing.
        UnexpectedColumn
            There is a column that is not 'set', 'interconnetor' or 'coefficient'.
        ColumnValues
            If there are inf or null values in the rhs coefficient.
        """
        if self.validate_inputs:
            self._validate_generic_interconnector_coefficients(interconnector_coefficients)
        self._generic_constraint_lhs['interconnectors'] = interconnector_coefficients

    def _validate_generic_interconnector_coefficients(self, interconnector_coefficients):
        schema = dv.DataFrameSchema(name='interconnector_coefficients', primary_keys=['set', 'interconnector'])
        schema.add_column(dv.SeriesSchema(name='set', data_type=str))
        schema.add_column(dv.SeriesSchema(name='interconnector', data_type=str))
        schema.add_column(dv.SeriesSchema(name='coefficient', data_type=np.float64, must_be_real_number=True))
        schema.validate(interconnector_coefficients)

    def make_constraints_elastic(self, constraints_key, violation_cost):
        """Make a set of constraints elastic, so they can be violated at a predefined cost.

        If an int or float is provided as the violation_cost, then this directly sets the cost. If a pd.DataFrame
        is provided then it must contain the columns 'set' and 'cost', 'set' is used to match the cost to
        the constraints, sets in the constraints that do not appear in the pd.DataFrame will not be made
        elastic.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['C', 'D'],
        ...     'region': ['X', 'X']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['X', 'Y'],
        ...                     unit_info=unit_info)

        Define a set of generic constraints and add them to the market.

        >>> generic_constraint_parameters = pd.DataFrame({
        ...   'set': ['A', 'B'],
        ...   'type': ['>=', '<='],
        ...   'rhs': [10.0, -100.0]})

        >>> market.set_generic_constraints(generic_constraint_parameters)

        Now the market should have a set of generic constraints.

        >>> print(market._constraints_rhs_and_type['generic'])
          set  constraint_id type    rhs
        0   A              0   >=   10.0
        1   B              1   <= -100.0

        Now these constraints can be made elastic.

        >>> market.make_constraints_elastic('generic', violation_cost=1000.0)

        Now the market will contain extra decision variables to capture the cost of violating the constraint.

        >>> print(market._decision_variables['generic_deficit'])
           variable_id  lower_bound  upper_bound        type
        0            0          0.0          inf  continuous
        1            1          0.0          inf  continuous

        >>> print(market._objective_function_components['generic_deficit'])
           variable_id    cost
        0            0  1000.0
        1            1  1000.0

        These will be mapped to the constraints

        >>> print(market._lhs_coefficients['generic_deficit'])
           variable_id  constraint_id  coefficient
        0            0              0          1.0
        1            1              1         -1.0

        If a pd.DataFrame is provided then we can set cost on a constraint basis.

        >>> violation_cost = pd.DataFrame({
        ...   'set': ['A', 'B'],
        ...   'cost': [1000.0, 2000.0]})

        >>> market.make_constraints_elastic('generic', violation_cost=violation_cost)

        >>> print(market._objective_function_components['generic_deficit'])
           variable_id    cost
        0            2  1000.0
        1            3  2000.0

        Note will the variable id get incremented with every use of the method only the latest set of variables are
        used.

        Parameters
        ----------
        constraints_key : str
            The key used to reference the constraint set in the dict self.market_constraints_rhs_and_type or
            self.constraints_rhs_and_type. See the documentation for creating the constraint set to get this key.

        violation_cost : str or float or int or pd.DataFrame

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If violation_cost is not str, numeric or pd.DataFrame.
        ModelBuildError
            If the constraint_key provided does not match any existing constraints.
        MissingColumnError
            If violation_cost is a pd.DataFrame and does not contain the columns set and cost.
            Or if the constraints to be made elastic do not have the set idenetifier.
        RepeatedRowError
            If violation_cost is a pd.DataFrame and has more than one row per set.
        ColumnDataTypeError
            If violation_cost is a pd.DataFrame and the column set is not str and the column
            cost is not numeric.
        """

        if constraints_key in self._market_constraints_rhs_and_type.keys():
            rhs_and_type = self._market_constraints_rhs_and_type[constraints_key].copy()
        elif constraints_key in self._constraints_rhs_and_type.keys():
            rhs_and_type = self._constraints_rhs_and_type[constraints_key].copy()
        else:
            raise check.ModelBuildError('constraints_key does not exist.')

        if isinstance(violation_cost, (int, float)) and not isinstance(violation_cost, bool):
            rhs_and_type['cost'] = violation_cost
        elif isinstance(violation_cost, pd.DataFrame):
            self._validate_violation_cost(violation_cost)
            rhs_and_type = pd.merge(rhs_and_type, violation_cost.loc[:, ['set', 'cost']], on='set')
        else:
            ValueError("Input for violation cost can only be numeric or a pd.Dataframe")

        if not rhs_and_type.empty:
            deficit_variables, lhs = elastic_constraints.create_deficit_variables(rhs_and_type, self._next_variable_id)
            self._decision_variables[constraints_key + '_deficit'] = \
                deficit_variables.loc[:, ['variable_id', 'lower_bound', 'upper_bound', 'type']]
            self._objective_function_components[constraints_key + '_deficit'] = \
                deficit_variables.loc[:, ['variable_id', 'cost']]
            self._lhs_coefficients[constraints_key + '_deficit'] = lhs
            self._next_variable_id = max(deficit_variables['variable_id']) + 1

    @staticmethod
    def _validate_violation_cost(violation_cost):
        schema = dv.DataFrameSchema(name='violation_cost', primary_keys=['set', 'cost'])
        schema.add_column(dv.SeriesSchema(name='set', data_type=str))
        schema.add_column(dv.SeriesSchema(name='cost', data_type=np.float64, must_be_real_number=True))
        schema.validate(violation_cost)

    def get_elastic_constraints_violation_degree(self, constraints_key):
        if constraints_key + '_deficit' in self._decision_variables:
            return self._decision_variables[constraints_key + '_deficit']['value'].sum()
        else:
            return 0.0

    def set_tie_break_constraints(self, cost):
        """Creates a cost that attempts to balance the energy dispatch of equally priced bids within a region.

        For each pair of bids from different generators in a region which are of the same price a constraint of the
        following form is created.

            B1 * 1/C1 - B2 * 1/C2 + D1 - D2 = 0

        Where B1 and B2 are the decision variables of each bid, C1 and C2 are the bid volumes, D1 and D2 are additional
        variables that have provided cost in the objective function. If a small cost (say 1e-6) is provided then this
        constraint balances the pro rata output of the bids.

        For AEMO documentation of this constraint
        `see AEMO doc <../../docs/pdfs/Schedule of Constraint Violation Penalty factors.pdf>` section 3 item 47.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['X', 'X']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['X'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [110.0, 150.0]})

        >>> market.set_unit_price_bids(price_bids)

        Creat tie break constraints.

        >>> market.set_tie_break_constraints(1e-3)

        This should add set of constraints rhs, type and lhs coefficients

        >>> market._decision_variables['bids']
          unit capacity_band service dispatch_type  variable_id  lower_bound  upper_bound        type
        0    A             1  energy     generator            0          0.0         20.0  continuous
        1    A             2  energy     generator            1          0.0         20.0  continuous
        2    A             3  energy     generator            2          0.0          5.0  continuous
        3    B             1  energy     generator            3          0.0         50.0  continuous
        4    B             2  energy     generator            4          0.0         30.0  continuous
        5    B             3  energy     generator            5          0.0         10.0  continuous

        >>> market._constraints_rhs_and_type['tie_break']
           constraint_id type  rhs
        0              0    =  0.0

        >>> market._lhs_coefficients['tie_break']
           constraint_id  variable_id  coefficient
        0              0            1         0.05
        0              0            3        -0.02

        And a set of deficiet variables that allow the constraints to violated at the specified cost.

        >>> market._lhs_coefficients['tie_break_deficit']
           variable_id  constraint_id  coefficient
        0            6              0         -1.0
        0            7              0          1.0

        >>> market._objective_function_components['tie_break_deficit']
           variable_id   cost
        0            6  0.001
        0            7  0.001

        """

        price_bids = self._objective_function_components['bids']
        bid_decision_variables = self._decision_variables['bids']
        unit_regions = self._unit_info.loc[:, ['unit', 'region']]

        lhs, rhs = unit_constraints.tie_break_constraints(price_bids, bid_decision_variables,
                                                          unit_regions, self._next_constraint_id)

        self._lhs_coefficients['tie_break'] = lhs
        self._constraints_rhs_and_type['tie_break'] = rhs
        self._next_constraint_id = rhs['constraint_id'].max() + 1
        self.make_constraints_elastic('tie_break', violation_cost=cost)

    def dispatch(self, energy_market_ceiling_price=None, energy_market_floor_price=None, fcas_market_ceiling_price=None,
                 allow_over_constrained_dispatch_re_run=False):
        """Combines the elements of the linear program and solves to find optimal dispatch.

        If allow_over_constrained_dispatch_re_run is set to True then constraints will be relaxed when market ceiling
        or floor prices are violated.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> market.set_unit_price_bids(price_bids)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create unit capacity based constraints.

        >>> market.set_demand_constraints(demand)

        Call the dispatch method.

        >>> market.dispatch()

        Now the market dispatch can be retrieved.

        >>> print(market.get_unit_dispatch())
          unit dispatch_type service  dispatch
        0    A     generator  energy      45.0
        1    B     generator  energy      55.0

        And the market prices can be retrieved.

        >>> print(market.get_energy_prices())
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
        if allow_over_constrained_dispatch_re_run:
            if (energy_market_ceiling_price is None or energy_market_floor_price is None or
                    fcas_market_ceiling_price is None):
                raise ValueError("""If allow_over_constrained_dispatch_re_run is set to True then values must \n
                                    be provided for energy_market_ceiling_price, energy_market_floor_price, and \n
                                    fcas_market_ceiling_price.""")

        # Create a data frame containing all fully defined components of the constraint matrix lhs. If there are none
        # then just create a place holder empty pd.DataFrame.
        if len(self._lhs_coefficients.values()) > 0:
            constraints_lhs = pd.concat(list(self._lhs_coefficients.values()))
        else:
            constraints_lhs = pd.DataFrame()

        # Get a pd.DataFrame mapping the generic constraint sets to their constraint ids.
        generic_constraint_ids = solver_interface.create_mapping_of_generic_constraint_sets_to_constraint_ids(
            self._constraints_rhs_and_type, self._market_constraints_rhs_and_type)

        # If there are any generic constraints create their lhs definitions.
        if generic_constraint_ids is not None:
            generic_lhs = []
            # If units have been added to the generic lhs then find the relevant variable ids and map them to the
            # constraint.
            if 'unit' in self._generic_constraint_lhs and 'bids' in self._variable_to_constraint_map['unit_level']:
                generic_constraint_units = self._generic_constraint_lhs['unit']
                unit_bids_to_constraint_map = self._variable_to_constraint_map['unit_level']['bids']
                unit_lhs = solver_interface.create_unit_level_generic_constraint_lhs(generic_constraint_units,
                                                                                     generic_constraint_ids,
                                                                                     unit_bids_to_constraint_map)
                generic_lhs.append(unit_lhs)
            # If regions have been added to the generic lhs then find the relevant variable ids and map them to the
            # constraint.
            if 'region' in self._generic_constraint_lhs and 'bids' in self._variable_to_constraint_map['regional']:
                generic_constraint_region = self._generic_constraint_lhs['region']
                unit_bids_to_constraint_map = self._variable_to_constraint_map['regional']['bids']
                regional_lhs = solver_interface.create_region_level_generic_constraint_lhs(generic_constraint_region,
                                                                                           generic_constraint_ids,
                                                                                           unit_bids_to_constraint_map)
                generic_lhs.append(regional_lhs)
            # If interconnectors have been added to the generic lhs then find the relevant variable ids and map them
            # to the constraint.
            if 'interconnectors' in self._generic_constraint_lhs and 'interconnectors' in self._decision_variables:
                generic_constraint_interconnectors = self._generic_constraint_lhs['interconnectors']
                interconnector_bids_to_constraint_map = self._decision_variables['interconnectors']
                interconnector_lhs = solver_interface.create_interconnector_generic_constraint_lhs(
                    generic_constraint_interconnectors, generic_constraint_ids, interconnector_bids_to_constraint_map)
                generic_lhs.append(interconnector_lhs)
            # Add the generic lhs definitions the cumulative lhs pd.DataFrame.
            constraints_lhs = pd.concat([constraints_lhs] + generic_lhs)

        # If there are constraints that have been defined on a regional basis then create the constraints lhs
        # definition by mapping to all the variables that have been defined for the corresponding region and service.
        if len(self._constraint_to_variable_map['regional']) > 0:
            constraints = pd.concat(list(self._constraint_to_variable_map['regional'].values()))
            decision_variables = pd.concat(list(self._variable_to_constraint_map['regional'].values()))
            regional_constraints_lhs = solver_interface.create_lhs(constraints, decision_variables,
                                                                   ['region', 'service'])
            # Add the lhs definitions the cumulative lhs pd.DataFrame.
            constraints_lhs = pd.concat([constraints_lhs, regional_constraints_lhs])

        # If there are constraints that have been defined on a unit basis then create the constraints lhs
        # definition by mapping to all the variables that have been defined for the corresponding unit and service.
        if len(self._constraint_to_variable_map['unit_level']) > 0:
            constraints = pd.concat(list(self._constraint_to_variable_map['unit_level'].values()))
            decision_variables = pd.concat(list(self._variable_to_constraint_map['unit_level'].values()))
            unit_constraints_lhs = solver_interface.create_lhs(
                constraints, decision_variables, ['unit', 'service', 'dispatch_type'])
            # Add the lhs definitions the cumulative lhs pd.DataFrame.
            constraints_lhs = pd.concat([constraints_lhs, unit_constraints_lhs])

        # Create the interface to the solver.
        si = solver_interface.InterfaceToSolver(self.solver_name)
        if self._decision_variables:
            # Combine dictionary of pd.DataFrames into a single pd.DataFrame for processing by the interface.
            variable_definitions = pd.concat(self._decision_variables)
            si.add_variables(variable_definitions)
        else:
            raise check.ModelBuildError('The market could not be dispatch because no variables have been created')

        # If Costs have been defined for bids or constraints then add an objective function.
        if self._objective_function_components:
            # Combine components of objective function into a single pd.DataFrame
            objective_function_definition = pd.concat(self._objective_function_components)
            si.add_objective_function(objective_function_definition)

        # Collect all constraint rhs and type definitions into a single pd.DataFrame.
        constraints_rhs_and_type = []
        if self._constraints_rhs_and_type:
            constraints_rhs_and_type.append(pd.concat(self._constraints_rhs_and_type))
        if self._market_constraints_rhs_and_type:
            constraints_rhs_and_type.append(pd.concat(self._market_constraints_rhs_and_type))
        if self._constraints_dynamic_rhs_and_type:
            constraints_dynamic_rhs_and_type = pd.concat(self._constraints_dynamic_rhs_and_type)
            # Create the rhs for the dynamic constraints.
            constraints_dynamic_rhs_and_type['rhs'] = constraints_dynamic_rhs_and_type. \
                apply(lambda x: si.variables[x['rhs_variable_id']], axis=1)
            constraints_rhs_and_type.append(constraints_dynamic_rhs_and_type)

        if len(constraints_rhs_and_type) > 0:
            constraints_rhs_and_type = pd.concat(constraints_rhs_and_type)
            si.add_constraints(constraints_lhs, constraints_rhs_and_type)

        # If interconnectors with losses are being used, create special ordered sets for modelling losses.
        if 'interpolation_weights' in self._decision_variables:
            special_ordered_sets = self._decision_variables['interpolation_weights']
            si.add_sos_type_2(special_ordered_sets, sos_id_columns=['interconnector', 'link'],
                              position_column='loss_segment')

        if 'interconnectors' in self._decision_variables:
            special_ordered_sets = self._decision_variables['interconnectors']
            special_ordered_sets = special_ordered_sets[
                special_ordered_sets['interconnector'] != special_ordered_sets['link']]
            if not special_ordered_sets.empty:
                special_ordered_sets = special_ordered_sets.rename(columns={'interconnector': 'sos_id'})
                si.add_sos_type_1(special_ordered_sets)

        si.optimize()

        # Find the slack in constraints.
        if self._constraints_rhs_and_type:
            for constraint_group in self._constraints_rhs_and_type:
                self._constraints_rhs_and_type[constraint_group]['slack'] = \
                    si.get_slack_in_constraints(self._constraints_rhs_and_type[constraint_group])
        if self._market_constraints_rhs_and_type:
            for constraint_group in self._market_constraints_rhs_and_type:
                self._market_constraints_rhs_and_type[constraint_group]['slack'] = \
                    si.get_slack_in_constraints(self._market_constraints_rhs_and_type[constraint_group])
        if self._constraints_dynamic_rhs_and_type:
            for constraint_group in self._constraints_dynamic_rhs_and_type:
                self._constraints_dynamic_rhs_and_type[constraint_group]['slack'] = \
                    si.get_slack_in_constraints(self._constraints_dynamic_rhs_and_type[constraint_group])

        # Get decision variable optimal values
        for var_group in self._decision_variables:
            self._decision_variables[var_group]['value'] = \
                si.get_optimal_values_of_decision_variables(self._decision_variables[var_group])

        # Models with interconnectors use binary variables, the model needs to be linearised to allow for shadow prices
        # to be accessed and used to price constraints.
        if 'interconnector_losses' in self._decision_variables:
            si = self._get_linear_model(si)
        si.linear_mip_model.optimize()

        for var_group in self._decision_variables:
            self._decision_variables[var_group]['value_lin'] = \
                si.get_optimal_values_of_decision_variables_lin(self._decision_variables[var_group])

        # If there are market constraints then calculate their associated prices.
        if self._market_constraints_rhs_and_type:
            for constraint_group in self._market_constraints_rhs_and_type:
                constraints_to_price = list(self._market_constraints_rhs_and_type[constraint_group]['constraint_id'])
                prices = si.price_constraints(constraints_to_price)
                self._market_constraints_rhs_and_type[constraint_group]['price'] = \
                    self._market_constraints_rhs_and_type[constraint_group]['constraint_id'].map(prices)

        if allow_over_constrained_dispatch_re_run:
            fcas_ceiling_price_violated = False
            if 'fcas' in self._market_constraints_rhs_and_type:
                if self._market_constraints_rhs_and_type['fcas']['price'].max() >= fcas_market_ceiling_price:
                    fcas_ceiling_price_violated = True

            energy_ceiling_price_violated = False
            if 'demand' in self._market_constraints_rhs_and_type:
                if self._market_constraints_rhs_and_type['demand']['price'].max() >= energy_market_ceiling_price:
                    energy_ceiling_price_violated = True

            energy_floor_price_violated = False
            if 'demand' in self._market_constraints_rhs_and_type:
                if self._market_constraints_rhs_and_type['demand']['price'].min() <= energy_market_floor_price:
                    energy_floor_price_violated = True

            deficit_variables = []
            lhs_deficit_variables = []

            generic_cons_violated = False
            if 'generic_deficit' in self._decision_variables:
                if (self._decision_variables['generic_deficit']['value'].max() > 0.0001 or
                        self._decision_variables['generic_deficit']['value'].min() < -0.0001):
                    generic_cons_violated = True
                    deficit_variables.append(self._decision_variables['generic_deficit'].copy())
                    lhs_deficit_variables.append(self._lhs_coefficients['generic_deficit'])

            fcas_cons_violated = False
            if 'fcas_deficit' in self._decision_variables:
                if (self._decision_variables['fcas_deficit']['value'].max() > 0.0001 or
                        self._decision_variables['fcas_deficit']['value'].min() < -0.0001):
                    fcas_cons_violated = True
                    deficit_variables.append(self._decision_variables['fcas_deficit'].copy())
                    lhs_deficit_variables.append(self._lhs_coefficients['fcas_deficit'])

            if ((fcas_ceiling_price_violated or energy_ceiling_price_violated or energy_floor_price_violated) and
                    (generic_cons_violated or fcas_cons_violated)):
                variables = pd.concat(deficit_variables)
                active_violation_variables = variables[(variables['value'] > 0.0) | (variables['value'] < -0.0)]
                lhs = pd.concat(lhs_deficit_variables)
                variables_and_cons = pd.merge(active_violation_variables, lhs, on='variable_id')
                variables_and_cons['adjuster'] = (variables_and_cons['value'] + 0.01) * \
                                                 variables_and_cons['coefficient'] * -1
                variables_and_cons.apply(lambda x: si.update_rhs(x['constraint_id'], x['adjuster']), axis=1)
                si.linear_mip_model.optimize()

                # If there are market constraints then calculate their associated prices.
                if self._market_constraints_rhs_and_type:
                    for constraint_group in self._market_constraints_rhs_and_type:
                        constraints_to_price = list(
                            self._market_constraints_rhs_and_type[constraint_group]['constraint_id'])
                        prices = si.price_constraints(constraints_to_price)
                        self._market_constraints_rhs_and_type[constraint_group]['price'] = \
                            self._market_constraints_rhs_and_type[constraint_group]['constraint_id'].map(prices)

        self.objective_value = si.mip_model.objective_value

    def _get_linear_model(self, si):
        self._remove_unused_interpolation_weights(si)
        self._disable_unused_link_pair(si)
        return si

    def _disable_unused_link_pair(self, si):
        inter_vars = self._decision_variables['interconnectors']
        inter_vars = inter_vars[inter_vars['interconnector'] != inter_vars['link']]
        inter_vars_unused = inter_vars[inter_vars['value'] == 0.0]
        si.disable_variables(inter_vars_unused)

    def _remove_unused_interpolation_weights(self, si):
        vars = pd.merge(self._decision_variables['interconnectors'].loc[:, ['interconnector', 'link', 'value']],
                        self._decision_variables['interpolation_weights'].loc[:, ['interconnector', 'link',
                                                                                  'break_point', 'variable_id']],
                        on=['interconnector', 'link'])
        vars['distance'] = (vars['value'] - vars['break_point']).abs()

        def not_closest_three(df):
            df = df.sort_values('distance')
            df = df.iloc[3:, :]
            return df

        vars_to_remove = vars.groupby(['interconnector', 'link'], as_index=False).apply(not_closest_three, include_groups=False)
        si.disable_variables(vars_to_remove.loc[:, ['variable_id']])

    def get_constraint_set_names(self):
        return list(self._market_constraints_rhs_and_type.keys()) + list(self._constraints_rhs_and_type.keys())

    def get_unit_dispatch(self):
        """Retrieves the energy dispatch for each unit.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> market.set_unit_price_bids(price_bids)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create unit capacity based constraints.

        >>> market.set_demand_constraints(demand)

        Call the dispatch method.

        >>> market.dispatch()

        Now the market dispatch can be retrieved.

        >>> print(market.get_unit_dispatch())
          unit dispatch_type service  dispatch
        0    A     generator  energy      45.0
        1    B     generator  energy      55.0

        Returns
        -------
        pd.DataFrame

        Raises
        ------
            ModelBuildError
                If a model build process is incomplete, i.e. there are energy bids but not energy demand set.
        """
        dispatch = self._decision_variables['bids'].loc[:, ['unit', 'dispatch_type', 'service', 'value']]
        dispatch.columns = ['unit', 'dispatch_type', 'service', 'dispatch']
        return dispatch.groupby(['unit', 'dispatch_type', 'service'], as_index=False).sum()

    def get_energy_prices(self):
        """Retrieves the energy price in each market region.

        Energy prices are the shadow prices of the demand constraint in each market region.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have two units called A and B, with three bid bands.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [20.0, 50.0],
        ...     '2': [20.0, 30.0],
        ...     '3': [5.0, 10.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     '1': [50.0, 100.0],
        ...     '2': [100.0, 130.0],
        ...     '3': [100.0, 150.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> market.set_unit_price_bids(price_bids)

        Define a demand level in each region.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW'],
        ...     'demand': [100.0]})

        Create unit capacity based constraints.

        >>> market.set_demand_constraints(demand)

        Call the dispatch method.

        >>> market.dispatch()

        Now the market prices can be retrieved.

        >>> print(market.get_energy_prices())
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
        prices = self._market_constraints_rhs_and_type['demand'].loc[:, ['region', 'price']]
        return prices

    def get_fcas_prices(self):
        """Retrives the price associated with each set of FCAS requirement constraints.

        Returns
        -------
        pd.DateFrame
        """
        prices = pd.merge(
            self._constraint_to_variable_map['regional']['fcas'].loc[:, ['service', 'region', 'constraint_id']],
            self._market_constraints_rhs_and_type['fcas'].loc[:, ['set', 'price', 'constraint_id']], on='constraint_id')
        prices = prices.groupby(['region', 'service'], as_index=False).aggregate({'price': 'sum'})
        return prices

    def get_interconnector_flows(self):
        """Retrieves the  flows for each interconnector.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW', 'VIC'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have just one unit that can provide 100 MW in NSW.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [100.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [80.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> market.set_unit_price_bids(price_bids)

        Define a demand level in each region, no power is required in NSW and 90.0 MW is required in VIC.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW', 'VIC'],
        ...     'demand': [0.0, 90.0]})

        Create unit capacity based constraints.

        >>> market.set_demand_constraints(demand)

        Define a an interconnector between NSW and VIC so generator can A can be used to meet demand in VIC.

        >>> interconnector = pd.DataFrame({
        ...     'interconnector': ['inter_one'],
        ...     'to_region': ['VIC'],
        ...     'from_region': ['NSW'],
        ...     'max': [100.0],
        ...     'min': [-100.0]})

        Create the interconnector.

        >>> market.set_interconnectors(interconnector)

        Call the dispatch method.

        >>> market.dispatch()

        Now the market dispatch can be retrieved.

        >>> print(market.get_unit_dispatch())
          unit dispatch_type service  dispatch
        0    A     generator  energy      90.0

        And the interconnector flows can be retrieved.

        >>> print(market.get_interconnector_flows())
          interconnector       link  flow
        0      inter_one  inter_one  90.0

        Returns
        -------
        pd.DataFrame

        """
        flow = self._decision_variables['interconnectors'].loc[:, ['interconnector', 'link', 'value']]
        flow.columns = ['interconnector', 'link', 'flow']

        if 'interconnector_losses' in self._decision_variables:
            losses = self._decision_variables['interconnector_losses'].loc[:, ['interconnector', 'link', 'value']]
            losses.columns = ['interconnector', 'link', 'losses']
            flow = pd.merge(flow, losses, 'left', on=['interconnector', 'link'])

        return flow.reset_index(drop=True)

    def get_region_dispatch_summary(self):
        """Calculates a dispatch summary at the regional level.

        Examples
        --------
        Define the unit information data set needed to initialise the market.

        >>> unit_info = pd.DataFrame({
        ...     'unit': ['A', 'B'],
        ...     'region': ['NSW', 'NSW']})

        Initialise the market instance.

        >>> market = SpotMarket(market_regions=['NSW', 'VIC'],
        ...                     unit_info=unit_info)

        Define a set of bids, in this example we have just one unit that can provide 100 MW in NSW.

        >>> volume_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [100.0]})

        Create energy unit bid decision variables.

        >>> market.set_unit_volume_bids(volume_bids)

        Define a set of prices for the bids.

        >>> price_bids = pd.DataFrame({
        ...     'unit': ['A'],
        ...     '1': [80.0]})

        Create the objective function components corresponding to the the energy bids.

        >>> market.set_unit_price_bids(price_bids)

        Define a demand level in each region, no power is required in NSW and 90.0 MW is required in VIC.

        >>> demand = pd.DataFrame({
        ...     'region': ['NSW', 'VIC'],
        ...     'demand': [0.0, 90.0]})

        Create unit capacity based constraints.

        >>> market.set_demand_constraints(demand)

        Define a an interconnector between NSW and VIC so generator can A can be used to meet demand in VIC.

        >>> interconnector = pd.DataFrame({
        ...     'interconnector': ['inter_one'],
        ...     'to_region': ['VIC'],
        ...     'from_region': ['NSW'],
        ...     'max': [100.0],
        ...     'min': [-100.0]})

        Create the interconnector.

        >>> market.set_interconnectors(interconnector)

        Define the interconnector loss function. In this case losses are always 5 % of line flow.

        >>> def constant_losses(flow=None):
        ...     return abs(flow) * 0.05

        Define the function on a per interconnector basis. Also details how the losses should be proportioned to the
        connected regions.

        >>> loss_functions = pd.DataFrame({
        ...    'interconnector': ['inter_one'],
        ...    'from_region_loss_share': [0.5],  # losses are shared equally.
        ...    'loss_function': [constant_losses]})

        Define the points to linearly interpolate the loss function between. In this example the loss function is
        linear so only three points are needed, but if a non linear loss function was used then more points would
        result in a better approximation.

        >>> interpolation_break_points = pd.DataFrame({
        ...    'interconnector': ['inter_one', 'inter_one', 'inter_one'],
        ...    'loss_segment': [1, 2, 3],
        ...    'break_point': [-120.0, 0.0, 100]})

        >>> market.set_interconnector_losses(loss_functions, interpolation_break_points)

        Call the dispatch method.

        >>> market.dispatch()

        Now the region dispatch summary can be retreived.

        >>> print(market.get_region_dispatch_summary())
          region   dispatch     inflow  transmission_losses  interconnector_losses
        0    NSW  94.615385 -92.307692                  0.0               2.307692
        1    VIC   0.000000  92.307692                  0.0               2.307692

        Returns
        -------
        pd.DataFrame

            =====================    =================================
            Columns:                 Description:
            region                   unique identifier of a market \n
                                     region, required (as `str`)
            dispatch                 the net dispatch of units inside \n
                                     a region i.e. generators dispatch \n
                                     minus load dispatch, in MW. (as `np.float64`)
            inflow                   the net inflow from interconnectors, \n
                                     not including losses, in MW \n
                                     (as `np.float64`)
            interconnector_losses    interconnector losses attributed \n
                                     to region, in MW, (as `np.float64`)
            =====================    =================================
        """
        dispatch_summary = self._get_net_unit_dispatch_by_region()
        if self._interconnectors_in_market():
            interconnector_inflow = self._get_interconnector_inflow_by_region()
            dispatch_summary = pd.merge(dispatch_summary, interconnector_inflow, how='outer', on='region')
            dispatch_summary = dispatch_summary.fillna(0.0)
            transmission_losses = self._get_transmission_losses()
            dispatch_summary = pd.merge(dispatch_summary, transmission_losses, on='region')
        if self._interconnectors_have_losses():
            interconnector_losses = self._get_interconnector_losses_by_region()
            dispatch_summary = pd.merge(dispatch_summary, interconnector_losses, on='region')
        return dispatch_summary

    def _get_net_unit_dispatch_by_region(self):

        unit_dispatch = self.get_unit_dispatch()
        unit_dispatch = unit_dispatch[unit_dispatch['service'] == 'energy']
        unit_dispatch_types = self._unit_info.loc[:, ['unit', 'region', 'dispatch_type']]
        unit_dispatch = pd.merge(
            unit_dispatch,
            unit_dispatch_types,
            on=['unit', 'dispatch_type']
        )

        def make_load_dispatch_negative(dispatch_type, dispatch):
            if dispatch_type == 'load':
                dispatch = -1 * dispatch
            return dispatch

        unit_dispatch['dispatch'] = \
            unit_dispatch.apply(lambda x: make_load_dispatch_negative(x['dispatch_type'], x['dispatch']), axis=1)

        unit_dispatch = unit_dispatch.groupby('region', as_index=False).aggregate({'dispatch': 'sum'})
        return unit_dispatch

    def _interconnectors_in_market(self):
        return self._interconnector_directions is not None

    def _get_interconnector_inflow_by_region(self):

        def calc_inflow_by_interconnector(interconnector_direction_coefficients, interconnector_flows):
            inflow = pd.merge(interconnector_direction_coefficients, interconnector_flows,
                              on=['interconnector', 'link'])
            inflow['inflow'] = inflow['flow'] * inflow['direction_coefficient']
            return inflow

        def calc_inflow_by_region(inflow):
            inflow = inflow.groupby('region', as_index=False).aggregate({'inflow': 'sum'})
            return inflow

        interconnector_flows = self.get_interconnector_flows()
        interconnector_direction_coefficients = self._get_interconnector_inflow_coefficients()
        inflow = calc_inflow_by_interconnector(interconnector_direction_coefficients, interconnector_flows)
        inflow = calc_inflow_by_region(inflow)

        return inflow

    def _get_interconnector_inflow_coefficients(self):

        def define_positive_inflows(interconnectors):
            inflow_direction = interconnectors.loc[:, ['interconnector', 'link', 'to_region']]
            inflow_direction['direction_coefficient'] = 1.0
            inflow_direction.columns = ['interconnector', 'link', 'region', 'direction_coefficient']
            return inflow_direction

        def define_negative_inflows(interconnectors):
            outflow_direction = interconnectors.loc[:, ['interconnector', 'link', 'from_region']]
            outflow_direction['direction_coefficient'] = -1.0
            outflow_direction.columns = ['interconnector', 'link', 'region', 'direction_coefficient']
            return outflow_direction

        positive = define_positive_inflows(self._interconnector_directions)
        negative = define_negative_inflows(self._interconnector_directions)

        return pd.concat([positive, negative])

    def _interconnectors_have_losses(self):
        return self._interconnector_loss_shares is not None

    def _get_interconnector_losses_by_region(self):
        from_region_loss_shares = self._get_from_region_loss_shares()
        to_region_loss_shares = self._get_to_region_loss_shares()
        loss_shares = pd.concat([from_region_loss_shares, to_region_loss_shares])
        losses = self.get_interconnector_flows().loc[:, ['interconnector', 'link', 'losses']]
        losses = pd.merge(losses, loss_shares, on=['interconnector', 'link'])
        losses['interconnector_losses'] = losses['losses'] * losses['loss_share']
        losses = losses.groupby('region', as_index=False).aggregate({'interconnector_losses': 'sum'})
        return losses

    def _get_from_region_loss_shares(self):
        from_region_loss_share = self._get_loss_shares('from_region')
        from_region_loss_share = from_region_loss_share.rename(columns={'from_region_loss_share': 'loss_share'})
        return from_region_loss_share

    def _get_to_region_loss_shares(self):
        to_region_loss_share = self._get_loss_shares('to_region')
        to_region_loss_share['loss_share'] = 1 - to_region_loss_share['from_region_loss_share']
        to_region_loss_share = to_region_loss_share.drop('from_region_loss_share', axis=1)
        return to_region_loss_share

    def _get_loss_shares(self, region_type):
        from_region_loss_share = self._interconnector_loss_shares
        regions = self._interconnector_directions.loc[:, ['interconnector', 'link', region_type]]
        regions = regions.rename(columns={region_type: 'region'})
        from_region_loss_share = pd.merge(from_region_loss_share, regions, on=['interconnector', 'link'])
        from_region_loss_share = from_region_loss_share.loc[:, ['interconnector', 'link', 'region',
                                                                'from_region_loss_share']]
        return from_region_loss_share

    def _get_transmission_losses(self):
        interconnector_directions = self._interconnector_directions
        loss_factors = hf.stack_columns(interconnector_directions, ['interconnector', 'link'],
                                        ['from_region_loss_factor', 'to_region_loss_factor'], 'direction',
                                        'loss_factor')
        interconnector_directions = hf.stack_columns(interconnector_directions, ['interconnector', 'link'],
                                                     ['to_region', 'from_region'], 'direction', 'region')
        loss_factors['direction'] = loss_factors['direction'].apply(lambda x: x.replace('_loss_factor', ''))
        loss_factors = pd.merge(loss_factors, interconnector_directions, on=['interconnector', 'link', 'direction'])
        flows_and_losses = self.get_interconnector_flows()
        flows_and_losses = pd.merge(flows_and_losses, loss_factors, on=['interconnector', 'link'])

        def calc_losses(direction, flow, loss_factor):
            if (direction == 'to_region' and flow >= 0.0) or (direction == 'from_region' and flow <= 0.0):
                losses = flow * (1 - loss_factor)
            elif (direction == 'to_region' and flow < 0.0) or (direction == 'from_region' and flow > 0.0):
                losses = abs(flow) - (abs(flow) / loss_factor)
            return losses

        flows_and_losses['transmission_losses'] = \
            flows_and_losses.apply(lambda x: calc_losses(x['direction'], x['flow'], x['loss_factor']), axis=1)
        flows_and_losses = flows_and_losses.groupby('region', as_index=False).aggregate({'transmission_losses': 'sum'})
        return flows_and_losses

    def get_fcas_availability(self):
        """Get the availability of fcas service on a unit level, after constraints.

        Examples
        --------
        Volume of each bid.

        >>> volume_bids = pd.DataFrame({
        ...   'unit': ['A', 'A', 'B', 'B', 'B'],
        ...   'service': ['energy', 'raise_6s', 'energy',
        ...               'raise_6s', 'raise_reg'],
        ...   '1': [100.0, 10.0, 110.0, 15.0, 15.0]})

        Price of each bid.

        >>> price_bids = pd.DataFrame({
        ...   'unit': ['A', 'A', 'B', 'B', 'B'],
        ...   'service': ['energy', 'raise_6s', 'energy',
        ...               'raise_6s', 'raise_reg'],
        ...   '1': [50.0, 35.0, 60.0, 20.0, 30.0]})

        Participant defined operational constraints on FCAS enablement.

        >>> fcas_trapeziums = pd.DataFrame({
        ...   'unit': ['B', 'B', 'A'],
        ...   'service': ['raise_reg', 'raise_6s', 'raise_6s'],
        ...   'max_availability': [15.0, 15.0, 10.0],
        ...   'enablement_min': [50.0, 50.0, 70.0],
        ...   'low_break_point': [65.0, 65.0, 80.0],
        ...   'high_break_point': [95.0, 95.0, 100.0],
        ...   'enablement_max': [110.0, 110.0, 110.0]})

        Unit locations.

        >>> unit_info = pd.DataFrame({
        ...   'unit': ['A', 'B'],
        ...   'region': ['NSW', 'NSW']})

        The demand in the regions being dispatched.

        >>> demand = pd.DataFrame({
        ...   'region': ['NSW'],
        ...   'demand': [195.0]})

        FCAS requirement in the regions being dispatched.

        >>> fcas_requirements = pd.DataFrame({
        ...   'set': ['nsw_regulation_requirement',
        ...           'nsw_raise_6s_requirement'],
        ...   'region': ['NSW', 'NSW'],
        ...   'service': ['raise_reg', 'raise_6s'],
        ...   'volume': [10.0, 10.0]})

        Create the market model with unit service bids.

        >>> market = SpotMarket(unit_info=unit_info,
        ...                     market_regions=['NSW'])
        >>> market.set_unit_volume_bids(volume_bids)
        >>> market.set_unit_price_bids(price_bids)

        Create constraints that enforce the top of the FCAS trapezium.

        >>> fcas_availability = fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]
        >>> market.set_fcas_max_availability(fcas_availability)

        Create constraints that enforce the lower and upper slope of the FCAS regulation service trapeziums.

        >>> regulation_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'raise_reg']
        >>> market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)

        Create constraints that enforce the lower and upper slope of the FCAS contingency
        trapezium. These constrains also scale slopes of the trapezium to ensure the
        co-dispatch of contingency and regulation services is technically feasible.

        >>> contingency_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'raise_6s']
        >>> market.set_joint_capacity_constraints(contingency_trapeziums)

        Set the demand for energy.

        >>> market.set_demand_constraints(demand)

        Set the required volume of FCAS services.

        >>> market.set_fcas_requirements_constraints(fcas_requirements)

        Calculate dispatch and pricing

        >>> market.dispatch()

        Return the total dispatch of each unit in MW.

        >>> print(market.get_unit_dispatch())
          unit dispatch_type    service  dispatch
        0    A     generator     energy     100.0
        1    A     generator   raise_6s       5.0
        2    B     generator     energy      95.0
        3    B     generator   raise_6s       5.0
        4    B     generator  raise_reg      10.0

        Return the constrained availability of each units fcas service.

        >>> print(market.get_fcas_availability())
          unit    service  availability
        0    A   raise_6s          10.0
        1    B   raise_6s           5.0
        2    B  raise_reg          10.0

        Returns
        -------

        """
        fcas_variable_slack = []
        for constraint_type in ['fcas_max_availability', 'joint_ramping_raise_reg', 'joint_ramping_lower_reg',
                                'joint_capacity', 'energy_and_regulation_capacity', 'bidirectional_ramp_up',
                                'bidirectional_ramp_down']:
            if constraint_type in self._constraints_rhs_and_type.keys():
                service_coefficients = self._constraint_to_variable_map['unit_level'][constraint_type]
                service_coefficients = service_coefficients.loc[:, ['constraint_id', 'unit', 'service', 'coefficient']]
                constraint_slack = self._constraints_rhs_and_type[constraint_type].loc[:, ['constraint_id', 'slack',
                                                                                           'type']]
                slack_temp = pd.merge(service_coefficients, constraint_slack, on='constraint_id')
                fcas_variable_slack.append(slack_temp)

        fcas_variable_slack = pd.concat(fcas_variable_slack)
        fcas_variable_slack['service_slack'] = \
            np.where(((fcas_variable_slack['coefficient'] < 0.0) & (fcas_variable_slack['type'] == '<=')) |
                     ((fcas_variable_slack['coefficient'] > 0.0) & (fcas_variable_slack['type'] == '>=')) |
                     ((fcas_variable_slack['coefficient'] < 0.00001) & (fcas_variable_slack['coefficient'] > -0.00001)),
                     np.inf, fcas_variable_slack['slack'].abs() / fcas_variable_slack['coefficient'].abs())
        fcas_variable_slack = \
            fcas_variable_slack.groupby(['unit', 'service'], as_index=False).aggregate({'service_slack': 'min'})
        fcas_variable_slack = fcas_variable_slack[fcas_variable_slack['service'] != 'energy']

        dispatch_levels = self.get_unit_dispatch()

        fcas_availability = pd.merge(fcas_variable_slack, dispatch_levels, on=['unit', 'service'])

        fcas_availability['availability'] = fcas_availability['dispatch'] + fcas_availability['service_slack']
        return fcas_availability.loc[:, ['unit', 'service', 'availability']]


class ModelBuildError(Exception):
    """Raise for building model components in wrong order."""


class MissingTable(Exception):
    """Raise for trying to access missing table."""
