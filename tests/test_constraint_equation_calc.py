import pytest
from datetime import datetime, timedelta
import random
import pandas as pd
import os

from nempy.historical_inputs.rhs_calculator import RHSCalc
from nempy.historical_inputs import xml_cache


def test_single_equation():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_rhs_calc_testing')
    xml_cache_manager.load_interval('2013/01/04 12:40:00')
    rhs_calculator = RHSCalc(xml_cache_manager)
    print(xml_cache_manager.get_file_path())
    con = 'DATASNAP'
    print(rhs_calculator.compute_constraint_rhs(con) - rhs_calculator.get_nemde_rhs(con))
    assert (rhs_calculator.compute_constraint_rhs(con) ==
            pytest.approx(rhs_calculator.get_nemde_rhs(con), 0.001))


def test_rhs_equations_in_order_of_length_all():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_rhs_calc_testing')

    def get_test_intervals(number=100):
        start_time = datetime(year=2013, month=1, day=1, hour=0, minute=0)
        end_time = datetime(year=2013, month=1, day=5, hour=0, minute=0)
        difference = end_time - start_time
        difference_in_5_min_intervals = difference.days * 12 * 24
        random.seed(1)
        intervals = random.sample(range(1, difference_in_5_min_intervals), number)
        times = [start_time + timedelta(minutes=5 * i) for i in intervals]
        times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
        return times_formatted

    intervals = []
    rhs_ids = []
    nemde_rhs = []
    nempy_rhs = []
    files = []
    basslink_dep_equations = set()

    test_context = 'online'  # set to local to run more extensive tests

    if test_context == 'online':
        intervals_to_test = ['2013/01/01 00:00:00']
    else:
        intervals_to_test = get_test_intervals(100)

    for interval in intervals_to_test:
        xml_cache_manager.load_interval(interval)
        rhs_calculator = RHSCalc(xml_cache_manager)
        rhs_equations = rhs_calculator.rhs_constraint_equations
        equations_in_length_order = sorted(rhs_equations, key=lambda k: len(rhs_equations[k]))
        xml_file = xml_cache_manager.get_file_path()
        for equation_id in equations_in_length_order:
            basslink_dep_equations = basslink_dep_equations.union(set(
                rhs_calculator.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W')))
            try:
                result = rhs_calculator.compute_constraint_rhs(equation_id)
                if not (rhs_calculator.get_nemde_rhs(equation_id) == pytest.approx(result, 0.001)):
                    intervals.append(interval)
                    rhs_ids.append(equation_id)
                    nemde_rhs.append(rhs_calculator.get_nemde_rhs(equation_id))
                    nempy_rhs.append(rhs_calculator.compute_constraint_rhs(equation_id))
                    files.append(xml_file)
            except:
                result = 'error'
                intervals.append(interval)
                rhs_ids.append(equation_id)
                nemde_rhs.append(rhs_calculator.get_nemde_rhs(equation_id))
                nempy_rhs.append(result)
                files.append(xml_file)

    errors = pd.DataFrame({
        'intervals': intervals,
        'rhs_id': rhs_ids,
        'nemde_rhs': nemde_rhs,
        'nempy_rhs': nempy_rhs,
        'files': files
    })

    error_raises = errors[errors['nempy_rhs'] == 'error'].copy()
    assert len(error_raises['rhs_id'].unique()) <= 4
    error_raises.to_csv('rhs_calculation_error_raises.csv')

    errors = errors[errors['nempy_rhs'] != 'error'].copy()

    errors['error'] = (errors['nemde_rhs'] - errors['nempy_rhs']).abs()

    errors.to_csv('rhs_calculation_errors.csv')

    basslink_errors = errors[errors['rhs_id'].isin(basslink_dep_equations)]
    basslink_errors.to_csv('basslink_rhs_calculation_errors.csv')

    errors_summary = errors.groupby('rhs_id', as_index=False).agg({'error': 'max'})
    errors_summary.to_csv('rhs_calculation_errors_summary.csv')

    basslink_dep_equations = list(basslink_dep_equations)

    basslink_errors_summary = errors_summary[errors_summary['rhs_id'].isin(basslink_dep_equations)]
    assert len(basslink_errors_summary) <= 5
    basslink_errors_summary.to_csv('rhs_calculation_basslink_errors_summary.csv')

    basslink_error_raises = error_raises[error_raises['rhs_id'].isin(basslink_dep_equations)]
    assert len(basslink_error_raises) == 0
    basslink_error_raises.to_csv('basslink_error_raises.csv')


def test_get_constraints_that_depend_on_500_GEN_INERTIA():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_rhs_calc_testing')
    xml_cache_manager.load_interval('2013/01/01 00:00:00')
    rhs_calculator = RHSCalc(xml_cache_manager)
    cons_that_depend_on_value = (
        rhs_calculator.get_rhs_constraint_equations_that_depend_value('500_GEN_INERTIA', 'A'))
    expected_cons = ["V::N_NIL_Q1", "V::N_NIL_Q2", "V::N_NIL_Q3", "V::N_NIL_Q4", "V::N_NIL_S1", "V::N_NIL_S2",
                     "V::N_NIL_S3", "V::N_NIL_S4", "V::N_NIL_V1", "V::N_NIL_V2", "V::N_NIL_V3", "V::N_NIL_V4"]
    assert expected_cons == cons_that_depend_on_value


def test_get_constraints_that_depend_on_BL_FREQ_ONSTATUS():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_rhs_calc_testing')
    xml_cache_manager.load_interval('2013/01/01 00:00:00')
    rhs_calculator = RHSCalc(xml_cache_manager)
    cons_that_depend_on_value = (
        rhs_calculator.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W'))
    # Intermediate generic equations
    # ["X_BL_MAIN_MAXAVL_OFF", "X_BL_MAIN_MAXAVL_ON", "X_BL_MAIN_MINAVL_OFF", "X_BL_MAIN_MINAVL_ON",
    #  "X_BL_TAS_MAXAVL_OFF", "X_BL_TAS_MAXAVL_ON", "X_BL_TAS_MINAVL_OFF", "X_BL_TAS_MINAVL_ON"]
    expected_cons = ["F_MAIN+NIL_DYN_RREG", "F_MAIN+NIL_MG_R5", "F_MAIN+NIL_MG_R6", "F_MAIN+NIL_MG_R60",
     "F_MAIN++NIL_DYN_RREG", "F_MAIN++NIL_MG_R5", "F_MAIN++NIL_MG_R6", "F_MAIN++NIL_MG_R60",
     "F_MAIN+ML_L5_0400", "F_MAIN+ML_L60_0400", "F_MAIN+ML_L6_0400", "F_MAIN+NIL_DYN_LREG",
     "F_MAIN++ML_L5_0400", "F_MAIN++ML_L60_0400", "F_MAIN++ML_L6_0400", "F_MAIN++NIL_DYN_LREG",
     "F_T+LREG_0050", "F_T+NIL_ML_L5", "F_T+NIL_ML_L6", "F_T+NIL_ML_L60", "F_T+NIL_TL_L5", "F_T+NIL_TL_L6",
     "F_T+NIL_TL_L60", "F_T++LREG_0050", "F_T++NIL_ML_L5", "F_T++NIL_ML_L6", "F_T++NIL_ML_L60", "F_T++NIL_TL_L5",
     "F_T++NIL_TL_L6", "F_T++NIL_TL_L60", "F_T+NIL_BB_TG_R5", "F_T+NIL_BB_TG_R6", "F_T+NIL_BB_TG_R60",
     "F_T+NIL_MG_R5", "F_T+NIL_MG_R6", "F_T+NIL_MG_R60", "F_T+NIL_WN_TG_R5", "F_T+NIL_WN_TG_R6", "F_T+NIL_WN_TG_R60",
     "F_T+RREG_0050",
     "F_T++NIL_BB_TG_R5", "F_T++NIL_BB_TG_R6", "F_T++NIL_BB_TG_R60", "F_T++NIL_MG_R5", "F_T++NIL_MG_R6",
     "F_T++NIL_MG_R60", "F_T++NIL_WN_TG_R5", "F_T++NIL_WN_TG_R6", "F_T++NIL_WN_TG_R60", "F_T++RREG_0050",
     "T_V_NIL_BL1", "V_T_NIL_BL1"]
    assert sorted(expected_cons) == sorted(cons_that_depend_on_value)






