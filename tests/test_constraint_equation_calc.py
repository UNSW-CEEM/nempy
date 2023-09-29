from nempy.historical_inputs.rhs_calculator import RHSCalc
from nempy.historical_inputs import xml_cache


def test_single_equation():
    xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2013')
    xml_cache_manager.load_interval('2013/01/01 00:00:00')
    rhs_calculator = RHSCalc(xml_cache_manager.xml)
    assert rhs_calculator.get_nemde_rhs('DATASNAP') == rhs_calculator.compute_constraint_rhs('DATASNAP')
