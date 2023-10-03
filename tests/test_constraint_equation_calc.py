import pytest

from nempy.historical_inputs.rhs_calculator import RHSCalc
from nempy.historical_inputs import xml_cache


def test_single_equation():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2013')
    xml_cache_manager.load_interval('2013/01/01 00:00:00')
    rhs_calculator = RHSCalc(xml_cache_manager)
    print(xml_cache_manager.get_file_path())
    assert (rhs_calculator.get_nemde_rhs('S>>V_NIL_TBSE_KHSG') ==
            pytest.approx(rhs_calculator.compute_constraint_rhs('S>>V_NIL_TBSE_KHSG'), 0.001))


def test_rhs_equations_in_order_of_length():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2013')
    xml_cache_manager.load_interval('2013/01/01 00:00:00')
    rhs_calculator = RHSCalc(xml_cache_manager)
    rhs_equations = rhs_calculator.rhs_constraint_equations
    equations_in_length_order = sorted(rhs_equations, key=lambda k: len(rhs_equations[k]))
    equation_no_generic_reference = rhs_calculator.get_rhs_equations_that_dont_reference_generic_equations()
    equations_in_length_order = [equ for equ in equations_in_length_order if equ in equation_no_generic_reference]
    print(xml_cache_manager.get_file_path())
    c = 1
    for equation_id in equations_in_length_order:
        # if equation_id in ['S>>V_NIL_TBSE_KHSG']:
        #     continue
        print(equation_id)
        print(c)
        c += 1
        assert (rhs_calculator.get_nemde_rhs(equation_id) ==
                pytest.approx(rhs_calculator.compute_constraint_rhs(equation_id), 0.001))