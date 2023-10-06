import pytest
from datetime import datetime, timedelta
import random
import pandas as pd

from nempy.historical_inputs.rhs_calculator import RHSCalc
from nempy.historical_inputs import xml_cache


'''
Test cases for sub groups: NRM_NSW1_QLD1

'''


def test_single_equation():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2013')
    xml_cache_manager.load_interval('2013/01/01 00:00:00')
    rhs_calculator = RHSCalc(xml_cache_manager)
    print(xml_cache_manager.get_file_path())
    assert (rhs_calculator.compute_constraint_rhs('V::N_NIL_V1') ==
            pytest.approx(rhs_calculator.get_nemde_rhs('V::N_NIL_V1'), 0.001))


def test_rhs_equations_in_order_of_length():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2013')
    xml_cache_manager.load_interval('2013/01/01 00:00:00')
    rhs_calculator = RHSCalc(xml_cache_manager)
    rhs_equations = rhs_calculator.rhs_constraint_equations
    equations_in_length_order = sorted(rhs_equations, key=lambda k: len(rhs_equations[k]))
    equation_no_generic_reference = rhs_calculator.get_rhs_equations_that_dont_reference_generic_equations()
    equations_in_length_order = [equ for equ in equations_in_length_order if equ in equation_no_generic_reference]
    print(xml_cache_manager.get_file_path()) # NEMSPDOutputs_2012123124000.loaded
    c = 1
    for equation_id in equations_in_length_order:
        if equation_id in ['Q^NIL_GC', 'V^S_HYCP', 'V::N_NIL_Q1', 'V::N_NIL_V1', 'V^SML_NIL_3', 'V^SML_NSWRB_2']:
            continue
        print(equation_id)
        print(c)
        c += 1
        assert (rhs_calculator.get_nemde_rhs(equation_id) ==
                pytest.approx(rhs_calculator.compute_constraint_rhs(equation_id), 0.0001))


def test_rhs_equations_in_order_of_length_all():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2013')

    def get_test_intervals(number=100):
        start_time = datetime(year=2012, month=1, day=1, hour=0, minute=0)
        end_time = datetime(year=2015, month=1, day=1, hour=0, minute=0)
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

    for interval in get_test_intervals(1000):
        xml_cache_manager.load_interval('2013/01/01 00:00:00')
        rhs_calculator = RHSCalc(xml_cache_manager)
        rhs_equations = rhs_calculator.rhs_constraint_equations
        equations_in_length_order = sorted(rhs_equations, key=lambda k: len(rhs_equations[k]))
        xml_file = xml_cache_manager.get_file_path()
        for equation_id in equations_in_length_order:
            # if equation_id in ['Q^NIL_GC', 'V^S_HYCP', 'V^SML_NIL_3', 'V^SML_NSWRB_2']:
            #     continue
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
    error_raises.to_csv('rhs_calculation_error_raises.csv')

    errors = errors[errors['nempy_rhs'] != 'error'].copy()

    errors['error'] = (errors['nemde_rhs'] - errors['nempy_rhs']).abs()

    errors.to_csv('rhs_calculation_errors.csv')

    errors_summary = errors.groupby('rhs_id', as_index=False).agg({'error': 'max'})
    errors_summary.to_csv('rhs_calculation_errors_summary.csv')

    basslink_dep_equations = list(basslink_dep_equations)

    basslink_errors_summary = errors_summary[errors_summary['rhs_id'].isin(basslink_dep_equations)]
    basslink_errors_summary.to_csv('rhs_calculation_basslink_errors_summary.csv')

    basslink_error_raises = error_raises[error_raises['rhs_id'].isin(basslink_dep_equations)]
    basslink_error_raises.to_csv('basslink_error_raises.csv')






