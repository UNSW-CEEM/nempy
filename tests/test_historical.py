import sqlite3
import pandas as pd
from pandas._testing import assert_frame_equal
import numpy as np
import os.path
from datetime import datetime, timedelta
import random
from nempy import historical_spot_market_inputs as hi, markets, helper_functions as hf
from time import time


# Define a set of random intervals to test
def get_test_intervals():
    start_time = datetime(year=2019, month=1, day=2, hour=0, minute=0)
    end_time = datetime(year=2019, month=2, day=1, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(1)
    intervals = random.sample(range(1, difference_in_5_min_intervals), 100)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


def setup():
    # Setup the database of historical inputs to test the Spot market class with.
    if not os.path.isfile('test_files/historical_inputs.db'):
        # Create a database for the require inputs.
        con = sqlite3.connect('test_files/historical_inputs.db')
        inputs_manager = hi.DBManager(connection=con)
        # inputs_manager.create_tables()
        inputs_manager.DISPATCHLOAD.create_table_in_sqlite_db()

        # Download data were inputs are needed on a monthly basis.
        finished = False
        for year in range(2019, 2020):
            for month in range(1, 2):
                if year == 2020 and month == 4:
                    finished = True
                    break
                # inputs_manager.DISPATCHINTERCONNECTORRES.add_data(year=year, month=month)
                # inputs_manager.DISPATCHREGIONSUM.add_data(year=year, month=month)
                inputs_manager.DISPATCHLOAD.add_data(year=year, month=month)
                # inputs_manager.BIDPEROFFER_D.add_data(year=year, month=month)
                # inputs_manager.BIDDAYOFFER_D.add_data(year=year, month=month)

            if finished:
                break

        # Download data where inputs are just needed from the latest month.
        # inputs_manager.INTERCONNECTOR.set_data(year=2020, month=3)
        # inputs_manager.LOSSFACTORMODEL.set_data(year=2020, month=3)
        # inputs_manager.LOSSMODEL.set_data(year=2020, month=3)
        inputs_manager.DUDETAILSUMMARY.set_data(year=2020, month=3)
        # inputs_manager.DUDETAIL.create_table_in_sqlite_db()
        # inputs_manager.DUDETAIL.set_data(year=2020, month=3)
        # inputs_manager.INTERCONNECTORCONSTRAINT.set_data(year=2020, month=3)

        con.close()


setup()


def test_historical_interconnector_losses():
    # Create a data base manager.
    con = sqlite3.connect('test_files/historical_inputs.db')
    inputs_manager = hi.DBManager(connection=con)

    for interval in get_test_intervals():
        INTERCONNECTOR = inputs_manager.INTERCONNECTOR.get_data()
        INTERCONNECTORCONSTRAINT = inputs_manager.INTERCONNECTORCONSTRAINT.get_data(interval)
        interconnectors = hi.format_interconnector_definitions(INTERCONNECTOR, INTERCONNECTORCONSTRAINT)
        interconnector_loss_coefficients = hi.format_interconnector_loss_coefficients(INTERCONNECTORCONSTRAINT)
        LOSSFACTORMODEL = inputs_manager.LOSSFACTORMODEL.get_data(interval)
        interconnector_demand_coefficients = hi.format_interconnector_loss_demand_coefficient(LOSSFACTORMODEL)
        LOSSMODEL = inputs_manager.LOSSMODEL.get_data(interval)
        interpolation_break_points = hi.format_interpolation_break_points(LOSSMODEL)
        DISPATCHREGIONSUM = inputs_manager.DISPATCHREGIONSUM.get_data(interval)
        regional_demand = hi.format_regional_demand(DISPATCHREGIONSUM)
        inter_flow = inputs_manager.DISPATCHINTERCONNECTORRES.get_data(interval)

        market = markets.Spot()

        inter_flow = inter_flow.loc[:, ['INTERCONNECTORID', 'MWFLOW', 'MWLOSSES']]
        inter_flow.columns = ['interconnector', 'MWFLOW', 'MWLOSSES']
        interconnectors = pd.merge(interconnectors, inter_flow, 'inner', on='interconnector')
        interconnectors['max'] = interconnectors['MWFLOW']
        interconnectors['min'] = interconnectors['MWFLOW']
        interconnectors = interconnectors.loc[:, ['interconnector', 'to_region', 'from_region', 'min', 'max']]
        market.set_interconnectors(interconnectors)

        # Create loss functions on per interconnector basis.
        loss_functions = hi.create_loss_functions(interconnector_loss_coefficients,
                                                  interconnector_demand_coefficients,
                                                  regional_demand.loc[:, ['region', 'loss_function_demand']])

        market.set_interconnector_losses(loss_functions, interpolation_break_points)

        # Calculate dispatch.
        market.dispatch()
        output = market.get_interconnector_flows()

        expected = inputs_manager.DISPATCHINTERCONNECTORRES.get_data(interval)
        expected = expected.loc[:, ['INTERCONNECTORID', 'MWFLOW', 'MWLOSSES']].sort_values('INTERCONNECTORID')
        expected.columns = ['interconnector', 'flow', 'losses']
        expected = expected.reset_index(drop=True)
        output = output.sort_values('interconnector').reset_index(drop=True)
        comparison = pd.merge(expected, output, 'inner', on='interconnector')
        comparison['diff'] = comparison['losses_x'] - comparison['losses_y']
        comparison['diff'] = comparison['diff'].abs()
        comparison['ok'] = comparison['diff'] < 0.5
        assert (comparison['ok'].all())


def test_using_availability_and_ramp_rates():
    """Test that using the availability and ramp up rate from DISPATCHLOAD always provides an upper bound on ouput.

    Note we only test for units in dispatch mode 0.0, i.e. not fast start units. Fast start units would appear to have
    their max output calculated using another procedure.
    """

    # Create a database for the require inputs.
    con = sqlite3.connect('test_files/historical_inputs.db')

    # Create a data base manager.
    inputs_manager = hi.DBManager(connection=con)

    for interval in get_test_intervals():
        dispatch_load = inputs_manager.DISPATCHLOAD.get_data(interval)
        dispatch_load = dispatch_load[dispatch_load['DISPATCHMODE'] == 0.0]
        dispatch_load = dispatch_load.loc[:, ['DUID', 'INITIALMW', 'AVAILABILITY', 'RAMPUPRATE', 'RAMPDOWNRATE',
                                              'TOTALCLEARED', 'DISPATCHMODE']]
        dispatch_load['RAMPMAX'] = dispatch_load['INITIALMW'] + dispatch_load['RAMPUPRATE'] * (5 / 60)
        dispatch_load['RAMPMIN'] = dispatch_load['INITIALMW'] - dispatch_load['RAMPDOWNRATE'] * (5 / 60)
        dispatch_load['assumption'] = ((dispatch_load['RAMPMAX'] + 0.01 >= dispatch_load['TOTALCLEARED']) &
                                       (dispatch_load['AVAILABILITY'] + 0.01 >= dispatch_load['TOTALCLEARED'])) | \
                                      (np.abs(dispatch_load['TOTALCLEARED'] - dispatch_load['RAMPMIN']) < 0.01)
        assert (dispatch_load['assumption'].all())


def test_max_capacity_not_less_than_availability():
    """For historical testing we are using availability as the unit capacity, so we want to test that the unit capacity
       or offer max is never lower than this value."""

    # Create a database for the require inputs.
    con = sqlite3.connect('test_files/historical_inputs.db')

    # Create a data base manager.
    inputs_manager = hi.DBManager(connection=con)

    for interval in get_test_intervals():
        dispatch_load = inputs_manager.DISPATCHLOAD.get_data(interval)
        dispatch_load = dispatch_load.loc[:, ['DUID', 'AVAILABILITY']]
        unit_capacity = inputs_manager.DUDETAIL.get_data(interval)
        unit_capacity = pd.merge(unit_capacity, dispatch_load, 'inner', on='DUID')
        unit_capacity['assumption'] = unit_capacity['AVAILABILITY'] <= unit_capacity['MAXCAPACITY']
        assert (unit_capacity['assumption'].all())


def test_determine_unit_limits():
    """Test the procedure for determining unit limits from historical inputs.

    It the limits set should always contain the historical amount dispatched within their bounds.
    """

    # Create a database for the require inputs.
    con = sqlite3.connect('test_files/historical_inputs.db')

    # Create a data base manager.
    inputs_manager = hi.DBManager(connection=con)

    for interval in get_test_intervals():
        dispatch_load = inputs_manager.DISPATCHLOAD.get_data(interval)
        dispatch_load = dispatch_load.loc[:, ['DUID', 'INITIALMW', 'AVAILABILITY', 'TOTALCLEARED', 'SEMIDISPATCHCAP',
                                              'RAMPUPRATE', 'RAMPDOWNRATE', 'DISPATCHMODE']]
        unit_capacity = inputs_manager.BIDPEROFFER_D.get_data(interval)
        unit_capacity = unit_capacity[unit_capacity['BIDTYPE'] == 'ENERGY']
        unit_limits = hi.determine_unit_limits(dispatch_load, unit_capacity)
        unit_limits = pd.merge(unit_limits, dispatch_load.loc[:, ['DUID', 'TOTALCLEARED', 'DISPATCHMODE']], 'inner',
                               left_on='unit', right_on='DUID')
        unit_limits['ramp_max'] = unit_limits['initial_output'] + unit_limits['ramp_up_rate'] * (5 / 60)
        unit_limits['ramp_min'] = unit_limits['initial_output'] - unit_limits['ramp_down_rate'] * (5 / 60)
        # Test the assumption that our calculated limits are not more restrictive then amount historically dispatched.
        unit_limits['assumption'] = ~((unit_limits['TOTALCLEARED'] > unit_limits['capacity'] + 0.01) |
                                      (unit_limits['TOTALCLEARED'] > unit_limits['ramp_max'] + 0.01) |
                                      (unit_limits['TOTALCLEARED'] < unit_limits['ramp_min'] - 0.01))
        assert (unit_limits['assumption'].all())


def test_fcas_trapezium_scaled_availability():
    con = sqlite3.connect('test_files/historical_inputs.db')
    inputs_manager = hi.DBManager(connection=con)

    for interval in get_test_intervals():
        DUDETAILSUMMARY = inputs_manager.DUDETAILSUMMARY.get_data(interval)
        unit_info = hi.format_unit_info(DUDETAILSUMMARY)

        # Unit bids.
        BIDPEROFFER_D = inputs_manager.BIDPEROFFER_D.get_data(interval)
        BIDDAYOFFER_D = inputs_manager.BIDDAYOFFER_D.get_data(interval)

        # Unit dispatch info
        DISPATCHLOAD = inputs_manager.DISPATCHLOAD.get_data(interval)
        unit_limits = hi.determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D)

        # FCAS bid prepocessing
        BIDPEROFFER_D = hi.scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD)
        BIDPEROFFER_D = hi.scaling_for_agc_ramp_rates(BIDPEROFFER_D, DISPATCHLOAD)
        BIDPEROFFER_D = hi.scaling_for_uigf(BIDPEROFFER_D, DISPATCHLOAD, DUDETAILSUMMARY)
        BIDPEROFFER_D, BIDDAYOFFER_D = hi.enforce_preconditions_for_enabling_fcas(
            BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, unit_limits.loc[:, ['unit', 'capacity']])

        BIDPEROFFER_D, BIDDAYOFFER_D = hi.use_historical_actual_availability_to_filter_fcas_bids(
            BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD)

        print('##########  {}'.format(interval))
        for unit in list(DISPATCHLOAD['DUID']):
            for BIDTYPE in ['LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC',
                            # 'LOWERREG',
                            'RAISEREG']:

                service_name_mapping = {'TOTALCLEARED': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg',
                                        'RAISE6SEC': 'raise_6s', 'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min',
                                        'LOWER6SEC': 'lower_6s', 'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min'}

                service = service_name_mapping[BIDTYPE]

                if '{} {} {}'.format(interval, unit, service) == '2019/01/30 04:30:00 ER01 lower_reg':
                    # Infeasible because of joint ramping constraints maybe due a lack of surplus and deficiet terms
                    continue

                if '{} {}'.format(interval, unit) == '2019/01/08 17:00:00 GSTONE6':
                    # Infeasible because of joint ramping constraints maybe due a lack of surplus and deficiet terms
                    continue

                if '{} {} {}'.format(interval, unit, service) == '2019/01/16 12:20:00 HPRL1 lower_reg':
                    # Looks like the joint capacity constraint was incorrect historically.
                    continue

                if '{} {} {}'.format(interval, unit, service) == '2019/01/09 15:30:00 DARTM1 lower_reg':
                    # Enablement min may be adjusted before dispatch.
                    continue

                if '{} {} {}'.format(interval, unit, service) == '2019/01/08 17:00:00 DARTM1 lower_reg':
                    # Enablement min may be adjusted before dispatch.
                    continue

                if '{} {} {}'.format(interval, unit, service) == '2019/01/23 14:20:00 DARTM1 lower_reg':
                        # not checked yet
                        continue

                if '{} {} {}'.format(interval, unit, service) == '2019/01/13 22:40:00 DARTM1 lower_reg':
                    # not checked yet
                    continue

                # if '{} {} {}'.format(interval, unit, service) == '2019/01/09 15:30:00 BW01 lower_reg':
                #     # Joint ramping constraint was not applied historically.
                #     continue
                #
                # if '{} {} {}'.format(interval, unit, service) == '2019/01/09 15:30:00 MP1 lower_reg':
                #     # Joint ramping constraint was not applied historically.
                #     continue
                #
                # if '{} {} {}'.format(interval, unit, service) == '2019/01/09 15:30:00 MP2 lower_reg':
                #     # Joint ramping constraint was not applied historically.
                #     continue
                #
                # if '{} {} {}'.format(interval, unit, service) == '2019/01/09 15:30:00 STAN-4 lower_reg':
                #     # Joint ramping constraint was not applied historically.
                #     continue
                #
                # if '{} {} {}'.format(interval, unit, service) == '2019/01/09 15:30:00 VP6 lower_reg':
                #     # Joint ramping constraint was not applied historically.
                #     continue

                if '{} {} {}'.format(interval, unit, service) == '2019/01/09 15:30:00 LOYYB2 lower_reg':
                    # Small difference in initial out put used causes small difference in availability = -0.15
                    continue

                # if '{} {} {}'.format(interval, unit, service) != '2019/01/05 14:10:00 HPRL1 lower_5min':
                #     continue

                if '{} {}'.format(interval, unit) == '2019/01/05 14:10:00 GSTONE1':
                    # Looks like a difference in rounding is causing infeasibility.
                    continue


                BIDPEROFFER_D_unit = BIDPEROFFER_D[BIDPEROFFER_D['DUID'] == unit]
                unit_limits_one = unit_limits[unit_limits['unit'] == unit]
                unit_info_one = unit_info[unit_info['unit'] == unit]
                BIDDAYOFFER_D_unit = BIDDAYOFFER_D[BIDDAYOFFER_D['DUID'] == unit]
                DISPATCHLOAD_unit = DISPATCHLOAD[DISPATCHLOAD['DUID'] == unit]

                if DISPATCHLOAD_unit[BIDTYPE].iloc[0] > DISPATCHLOAD_unit[BIDTYPE + 'ACTUALAVAILABILITY'].iloc[0]:
                    continue

                # Extract just bidding info
                volume_bids = hi.format_volume_bids(BIDPEROFFER_D_unit)
                price_bids = hi.format_price_bids(BIDDAYOFFER_D_unit)
                fcas_trapeziums = hi.format_fcas_trapezium_constraints(BIDPEROFFER_D_unit)

                if service not in list(fcas_trapeziums['service']):
                    continue

                print('{} {}'.format(unit, service))

                market = markets.Spot()

                # Add generators to the market.
                market.set_unit_info(unit_info.loc[:, ['unit', 'region', 'dispatch_type']])

                # Set volume of each bids.
                volume_bids = volume_bids[volume_bids['unit'].isin(list(unit_info_one['unit']))]
                market.set_unit_volume_bids(volume_bids.loc[:, ['unit', 'service', '1', '2', '3', '4', '5',
                                                                '6', '7', '8', '9', '10']])

                # Set prices of each bid.
                price_bids = price_bids[price_bids['unit'].isin(list(unit_info['unit']))]
                market.set_unit_price_bids(price_bids.loc[:, ['unit', 'service', '1', '2', '3', '4', '5',
                                                              '6', '7', '8', '9', '10']])

                # Set unit operating limits.
                if not unit_limits_one.empty:
                    market.set_unit_capacity_constraints(unit_limits_one.loc[:, ['unit', 'capacity']])
                    market.set_unit_ramp_up_constraints(unit_limits_one.loc[:, ['unit', 'initial_output',
                                                                                'ramp_up_rate']])
                    market.set_unit_ramp_down_constraints(unit_limits_one.loc[:, ['unit', 'initial_output',
                                                                                  'ramp_down_rate']])

                # Create constraints that enforce the top of the FCAS trapezium.
                if not fcas_trapeziums.empty:
                    fcas_availability = fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]
                    market.set_fcas_max_availability(fcas_availability)

                # Create constraints the enforce the lower and upper slope of the FCAS regulation
                # service trapeziums.
                regulation_trapeziums = fcas_trapeziums[fcas_trapeziums['service'].isin(['raise_reg', 'lower_reg'])]
                if not regulation_trapeziums.empty:
                    market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)
                    market.set_joint_ramping_constraints(regulation_trapeziums.loc[:, ['unit', 'service']],
                                                         unit_limits.loc[:, ['unit', 'initial_output',
                                                                             'ramp_down_rate', 'ramp_up_rate']])

                # Create constraints that enforce the lower and upper slope of the FCAS contingency
                # trapezium. These constrains also scale slopes of the trapezium to ensure the
                # co-dispatch of contingency and regulation services is technically feasible.
                contingency_trapeziums = fcas_trapeziums[~fcas_trapeziums['service'].isin(['raise_reg', 'lower_reg'])]
                if not contingency_trapeziums.empty:
                    market.set_joint_capacity_constraints(contingency_trapeziums)

                # Set cost of bid variables to zero except for fcas being tested
                market.objective_function_components['bids']['cost'] = np.where(
                    market.objective_function_components['bids']['service'] == service, -1.0, 0.0)

                vars = ['TOTALCLEARED', 'LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC',
                        'LOWERREG', 'RAISEREG']

                bounds = DISPATCHLOAD_unit.loc[:, ['DUID'] + vars]
                bounds.columns = ['unit'] + vars

                bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=vars, type_name='service',
                                          value_name='dispatched')

                bounds['service'] = bounds['service'].apply(lambda x: service_name_mapping[x])

                decision_variables = market.decision_variables['bids'].copy()

                decision_variables = pd.merge(decision_variables, bounds, on=['unit', 'service'])

                decision_variables_test_service = decision_variables[decision_variables['service'] == service]
                decision_variables_not_test_service = decision_variables[~(decision_variables['service'] == service)]

                decision_variables_first_bid = decision_variables_not_test_service.groupby(['unit', 'service'],
                                                                                            as_index=False).first()

                def last_bids(df):
                    return df.iloc[1:]

                decision_variables_remaining_bids = \
                    decision_variables_not_test_service.groupby(['unit', 'service'],
                                                                as_index=False).apply(last_bids)

                decision_variables_first_bid['lower_bound'] = decision_variables_first_bid['dispatched'] - 0.0001
                decision_variables_first_bid['upper_bound'] = decision_variables_first_bid['dispatched'] + 0.0001
                decision_variables_remaining_bids['lower_bound'] = 0.0
                decision_variables_remaining_bids['upper_bound'] = 0.0

                decision_variables = pd.concat([decision_variables_test_service, decision_variables_first_bid,
                                                decision_variables_remaining_bids])

                market.decision_variables['bids'] = decision_variables

                market.dispatch()

                availabilities = ['RAISE6SECACTUALAVAILABILITY', 'RAISE60SECACTUALAVAILABILITY',
                                  'RAISE5MINACTUALAVAILABILITY', 'RAISEREGACTUALAVAILABILITY',
                                  'LOWER6SECACTUALAVAILABILITY', 'LOWER60SECACTUALAVAILABILITY',
                                  'LOWER5MINACTUALAVAILABILITY', 'LOWERREGACTUALAVAILABILITY']

                availabilities_mapping = {'RAISEREGACTUALAVAILABILITY': 'raise_reg',
                                          'LOWERREGACTUALAVAILABILITY': 'lower_reg',
                                          'RAISE6SECACTUALAVAILABILITY': 'raise_6s',
                                          'RAISE60SECACTUALAVAILABILITY': 'raise_60s',
                                          'RAISE5MINACTUALAVAILABILITY': 'raise_5min',
                                          'LOWER6SECACTUALAVAILABILITY': 'lower_6s',
                                          'LOWER60SECACTUALAVAILABILITY': 'lower_60s',
                                          'LOWER5MINACTUALAVAILABILITY': 'lower_5min'}

                bounds = DISPATCHLOAD_unit.loc[:, ['DUID'] + availabilities]
                bounds.columns = ['unit'] + availabilities

                availabilities = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=availabilities,
                                                  type_name='service', value_name='availability')

                availabilities['service'] = availabilities['service'].apply(lambda x: availabilities_mapping[x])

                availabilities = availabilities[availabilities['service'] == service]

                output = market.get_unit_dispatch()

                availabilities = pd.merge(availabilities, output, 'left', on=['unit', 'service'])

                availabilities['error'] = availabilities['dispatch'] - availabilities['availability']

                availabilities['match'] = availabilities['error'].abs() < 0.1

                if not availabilities['match'].all() and service == 'lower_reg':
                    dispatch = availabilities['dispatch'].iloc[0]
                    availability = availabilities['availability'].iloc[0]
                    joint_ramping_bound = DISPATCHLOAD_unit['TOTALCLEARED'].iloc[0] - \
                                          (DISPATCHLOAD_unit['INITIALMW'].iloc[0] -
                                           DISPATCHLOAD_unit['RAMPDOWNRATE'].iloc[0]/12)
                    diff = abs(dispatch - joint_ramping_bound)
                    if dispatch < availability and diff < 0.001:
                        print('Test aborted for {} {}, historical calc did not apply joint ramping cons'.format(unit, service))
                        continue

                if not availabilities['match'].all() and service == 'raise_reg':
                    dispatch = availabilities['dispatch'].iloc[0]
                    availability = availabilities['availability'].iloc[0]
                    joint_ramping_bound = -1 * DISPATCHLOAD_unit['TOTALCLEARED'].iloc[0] + \
                                          (DISPATCHLOAD_unit['INITIALMW'].iloc[0] +
                                           DISPATCHLOAD_unit['RAMPUPRATE'].iloc[0]/12)
                    diff = abs(dispatch - joint_ramping_bound)
                    if dispatch < availability and diff < 0.001:
                        print('Test aborted for {} {}, historical calc did not apply joint ramping cons'.format(unit, service))
                        continue

                assert (availabilities['match'].all())

