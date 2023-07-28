from nempy.historical_inputs.rhs_calculator import rpn_calc


def test_no_rpn_operators():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.2"""

    equation = [
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '-1', '@Value': '500'},
        {'@TermID': '2', '@SpdID': 'BW01.NBAY1', '@SpdType': 'I', '@Multiplier': '0.5', '@Value': '-1000'},
        {'@TermID': '3', '@SpdID': 'BW01.NBAY1', '@SpdType': 'R', '@Multiplier': '1', '@Value': '10000'},
    ]

    assert rpn_calc(equation) == 9000


def test_groups():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.3"""

    equation = [
        {'@TermID': '1', '@GroupID': '5', '@SpdID': 'NRATSE_MNDT8', '@SpdType': 'E', '@Multiplier': '1', '@Value': '1000'},
        {'@TermID': '2', '@GroupID': '5', '@SpdID': 'MW_MN_8', '@SpdType': 'A', '@Multiplier': '-1', '@Value': '400'},
        {'@TermID': '3', '@GroupID': '5', '@SpdID': 'MW_MN_16', '@SpdType': 'A', '@Multiplier': '-0.498', '@Value': '500'},
        {'@TermID': '4', '@GroupID': '5', '@SpdID': 'Operating_Margin', '@SpdType': 'C', '@Multiplier': '-25'},
        {'@TermID': '5', '@SpdID': 'HEADROOM', '@SpdType': 'G', '@Multiplier': '4.197'},
        {'@TermID': '6', '@SpdID': 'TALWA1.NDT13T', '@SpdType': 'T', '@Multiplier': '-1', '@Value': '250'},
    ]

    assert rpn_calc(equation) == 1118.222


def test_top_stack_element():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.5"""

    equation = [
        {'@TermID': '1', '@SpdID': 'NRATSE_MNDT8', '@SpdType': 'E', '@Multiplier': '1', '@Value': '1000'},
        {'@TermID': '2', '@SpdID': 'MW_MN_8', '@SpdType': 'A', '@Multiplier': '-1', '@Value': '400'},
        {'@TermID': '3', '@SpdID': 'MW_MN_16', '@SpdType': 'A', '@Multiplier': '-0.498', '@Value': '500'},
        {'@TermID': '4', '@SpdID': 'Operating_Margin', '@SpdType': 'C', '@Multiplier': '-25'},
        {'@TermID': '5', '@SpdID': 'HEADROOM', '@SpdType': 'U', '@Multiplier': '4.197'},
        {'@TermID': '6', '@SpdID': 'TALWA1.NDT13T', '@SpdType': 'T', '@Multiplier': '-1', '@Value': '250'},
    ]

    assert rpn_calc(equation) == 1118.222