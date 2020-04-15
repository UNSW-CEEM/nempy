from nempy import check, market_constraints, objective_function, solver_interface, unit_constraints, variable_ids


class RealTime:
    def __init__(self, unit_info, dispatch_interval=None):
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

    @check.one_one_row_per_unit
    def set_unit_energy_volume_bids(self, volume_bids):
        """Control layer method, handles the creation of decision variables corresponding to energy bids.

        1. Create unit variable ids: create a decision variable for each units bid, bids of zero MW are dropped.
        2. Update the variable id counter: the next available integer to be used as an id.

        :param volume_bids: DataFrame
            unit: str
                The unique name of each unit
            1: float
                First bid band volume in MW
            2: float
                second bid band volume in MW
            n: float
                n th bid band volume in MW
        :return:
        """
        # 1. Create unit variable ids
        self.decision_variables['energy_units'] = variable_ids.energy(volume_bids, self.next_variable_id)
        # 2. Update the variable id counter:
        self.next_variable_id = max(self.decision_variables['energy_units']['variable_id']) + 1

    @check.energy_bid_ids_exist
    @check.one_one_row_per_unit
    def set_unit_energy_price_bids(self, price_bids):
        """Control layer method, handles the creation of objective function costs corresponding to energy bids.

        :param price_bids: DataFrame
            unit: str
                The unique name of each unit
            1: float
                First bid band volume in MW
            2: float
                second bid band volume in MW
            n: float
                n th bid band volume in MW
        :return:
        """
        self.objective_function_components['energy_bids'] = objective_function.energy(
            self.decision_variables['energy_units'], price_bids, self.unit_info)

    @check.energy_bid_ids_exist
    @check.one_one_row_per_unit
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
        lhs_coefficients, rhs_and_type = unit_constraints.capacity(self.decision_variables['energy_units'], unit_limits,
                                                                   self.next_constraint_id)
        # 2. Save constraint details.
        self.constraints_lhs_coefficients['unit_capacity'] = lhs_coefficients
        self.constraints_rhs_and_type['unit_capacity'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(lhs_coefficients['constraint_id']) + 1

    @check.energy_bid_ids_exist
    @check.one_one_row_per_unit
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
        lhs_coefficients, rhs_and_type = unit_constraints.ramp_up(self.decision_variables['energy_units'], unit_limits,
                                                                  self.next_constraint_id, self.dispatch_interval)
        # 2. Save constraint details.
        self.constraints_lhs_coefficients['ramp_up'] = lhs_coefficients
        self.constraints_rhs_and_type['ramp_up'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(lhs_coefficients['constraint_id']) + 1

    @check.energy_bid_ids_exist
    @check.one_one_row_per_unit
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
        lhs_coefficients, rhs_and_type = unit_constraints.ramp_down(self.decision_variables['energy_units'], unit_limits,
                                                                  self.next_constraint_id, self.dispatch_interval)
        # 2. Save constraint details.
        self.constraints_lhs_coefficients['ramp_down'] = lhs_coefficients
        self.constraints_rhs_and_type['ramp_down'] = rhs_and_type
        # 3. Update the constraint and variable id counter
        self.next_constraint_id = max(lhs_coefficients['constraint_id']) + 1

    @check.energy_bid_ids_exist
    @check.one_one_row_per_region
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
        lhs_coefficients, rhs_and_type = market_constraints.energy(self.decision_variables['energy_units'],
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
        dispatch = self.decision_variables['energy_units'].loc[:, ['unit', 'value']]
        dispatch.columns = ['unit', 'dispatch']
        return dispatch.groupby('unit', as_index=False).sum()

    def get_energy_prices(self):
        prices = self.market_constraints_rhs_and_type['energy_market'].loc[:, ['region', 'price']]
        return prices



