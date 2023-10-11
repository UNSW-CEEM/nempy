from nempy.historical_inputs.rhs_calculator import _rpn_calc, _rpn_stack


def test_no_rpn_operators():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.2"""

    equation = [
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '-1', '@Value': '500'},
        {'@TermID': '2', '@SpdID': 'BW01.NBAY1', '@SpdType': 'I', '@Multiplier': '0.5', '@Value': '-1000'},
        {'@TermID': '3', '@SpdID': 'BW01.NBAY1', '@SpdType': 'R', '@Multiplier': '1', '@Value': '10000'},
    ]

    assert _rpn_calc(equation) == 9000


def test_groups():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.3"""

    equation = [
        {'@TermID': '1', '@GroupTerm': '5', '@SpdID': 'NRATSE_MNDT8', '@SpdType': 'E', '@Multiplier': '1', '@Value': '1000'},
        {'@TermID': '2', '@GroupTerm': '5', '@SpdID': 'MW_MN_8', '@SpdType': 'A', '@Multiplier': '-1', '@Value': '400'},
        {'@TermID': '3', '@GroupTerm': '5', '@SpdID': 'MW_MN_16', '@SpdType': 'A', '@Multiplier': '-0.498', '@Value': '500'},
        {'@TermID': '4', '@GroupTerm': '5', '@SpdID': 'Operating_Margin', '@SpdType': 'C', '@Multiplier': '-25'},
        {'@TermID': '5', '@SpdID': 'HEADROOM', '@SpdType': 'G', '@Multiplier': '4.197'},
        {'@TermID': '6', '@SpdID': 'TALWA1.NDT13T', '@SpdType': 'T', '@Multiplier': '-1', '@Value': '250'},
    ]

    assert _rpn_calc(equation) == 1118.222


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

    assert _rpn_calc(equation) == 1118.222


def test_step_one_of_two():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.6.1"""

    equation = [
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'STEP',
         '@Value': '500'},
        {'@TermID': '2', '@SpdID': 'BW02.NBAY2', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'STEP',
         '@Value': '0'}
    ]

    assert _rpn_calc(equation) == 1


def test_step_two_of_two():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.6.1"""

    equation = [
        {'@TermID': '1', '@SpdID': 'CONSTANT', '@SpdType': 'C', '@Multiplier': '2'},
        {'@TermID': '2', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'PUSH',
         '@Value': '400'},
        {'@TermID': '2', '@SpdID': '', '@SpdType': 'U', '@Multiplier': '500', '@Operation': 'STEP'},
        {'@TermID': '2', '@SpdID': '', '@SpdType': 'U', '@Multiplier': '1', '@Operation': 'ADD'}
    ]

    assert _rpn_calc(equation) == 502


def test_square():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.6.2"""

    equation = [
        {'@TermID': '1', '@SpdID': 'NSW1-QLD1 ', '@SpdType': 'I', '@Multiplier': '1', '@Operation': 'POW2',
         '@Value': '100'}
    ]

    assert _rpn_calc(equation) == 10000


def test_cube():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.6.3"""

    equation = [
        {'@TermID': '1', '@SpdID': 'NSW1-QLD1 ', '@SpdType': 'I', '@Multiplier': '1', '@Operation': 'POW3',
         '@Value': '100'}
    ]

    assert _rpn_calc(equation) == 1000000


def test_sqrt():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.6.4"""

    equation = [
        {'@TermID': '1', '@SpdID': 'NSW1-QLD1 ', '@SpdType': 'I', '@Multiplier': '1', '@Operation': 'SQRT',
         '@Value': '100'}
    ]

    assert _rpn_calc(equation) == 10


def test_absolute_value():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.6.5"""

    equation = [
        {'@TermID': '1', '@SpdID': 'NSW1-QLD1 ', '@SpdType': 'I', '@Multiplier': '1', '@Operation': 'ABS',
         '@Value': '-100'}
    ]

    assert _rpn_calc(equation) == 100


def test_negation():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.6.6"""

    equation = [
        {'@TermID': '1', '@SpdID': 'NSW1-QLD1', '@SpdType': 'I', '@Multiplier': '1', '@Operation': 'NEG',
         '@Value': '100'}
    ]

    assert _rpn_calc(equation) == -100


def test_add():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.7.1"""

    equation = [
        {'@TermID': '1', '@SpdID': 'MW_MN_8', '@SpdType': 'A', '@Multiplier': '1', '@Value': '100'},
        {'@TermID': '1', '@SpdID': 'MW_MN_16', '@SpdType': 'A', '@Multiplier': '2', '@Operation': 'ADD',
         '@Value': '200'}
    ]

    assert _rpn_calc(equation) == 600


def test_sub():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.7.2"""

    equation = [
        {'@TermID': '1', '@SpdID': 'MW_MN_8', '@SpdType': 'A', '@Multiplier': '1', '@Value': '100'},
        {'@TermID': '1', '@SpdID': 'MW_MN_16', '@SpdType': 'A', '@Multiplier': '2', '@Operation': 'SUB',
         '@Value': '200'}
    ]

    assert _rpn_calc(equation) == -200


def test_multiply():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.7.3"""

    equation = [
        {'@TermID': '1', '@SpdID': 'MW_MN_8', '@SpdType': 'A', '@Multiplier': '1', '@Value': '10'},
        {'@TermID': '1', '@SpdID': 'MW_MN_16', '@SpdType': 'A', '@Multiplier': '2', '@Operation': 'MUL',
         '@Value': '20'}
    ]

    assert _rpn_calc(equation) == 400


def test_divide():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.7.4"""

    equation = [
        {'@TermID': '1', '@SpdID': 'MW_MN_8', '@SpdType': 'A', '@Multiplier': '1', '@Value': '10'},
        {'@TermID': '1', '@SpdID': 'MW_MN_16', '@SpdType': 'A', '@Multiplier': '2', '@Operation': 'DIV',
         '@Value': '20'}
    ]

    assert _rpn_calc(equation) == 1.0


def test_max():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.7.5"""

    equation = [
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '1', '@Value': '500'},
        {'@TermID': '1', '@SpdID': 'BW02.NBAY2', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'MAX',
         '@Value': '650'},
        # {'@TermID': '1', '@SpdID': 'ER01.NEPS1', '@SpdType': 'U', '@Multiplier': '1', '@Operation': 'MAX',
        #  '@Value': '670'},
        # {'@TermID': '1', '@SpdID': 'TALWA1.NDT13T', '@SpdType': 'U', '@Multiplier': '1', '@Operation': 'MAX',
        #  '@Value': '250'}
    ]

    assert _rpn_calc(equation) == 650


def test_min():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.7.6"""

    equation = [
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '1', '@Value': '500'},
        {'@TermID': '1', '@SpdID': 'BW02.NBAY2', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'MIN',
         '@Value': '650'},
        # {'@TermID': '1', '@SpdID': 'ER01.NEPS1', '@SpdType': 'U', '@Multiplier': '1', '@Operation': 'MAX',
        #  '@Value': '670'},
        # {'@TermID': '1', '@SpdID': 'TALWA1.NDT13T', '@SpdType': 'U', '@Multiplier': '1', '@Operation': 'MAX',
        #  '@Value': '250'}
    ]

    assert _rpn_calc(equation) == 500


def test_push():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.8.1"""

    equation = [
        {'@TermID': '1', '@SpdID': 'NSW1-QLD1', '@SpdType': 'I', '@Multiplier': '1', '@Value': '100'},
        {'@TermID': '1', '@SpdID': 'TALWA1.NDT13T', '@SpdType': 'T', '@Multiplier': '0.5', '@Operation': 'PUSH',
         '@Value': '350'},
    ]

    assert _rpn_stack(equation) == [175.0, 100]

    assert _rpn_calc(equation) == 175.0


def test_duplicate():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.8.2"""

    equation = [
        {'@TermID': '1', '@SpdID': 'NSW1-QLD1', '@SpdType': 'I', '@Multiplier': '1', '@Value': '200'},
        {'@TermID': '1', '@SpdType': 'U', '@Multiplier': '0.5', '@Operation': 'DUP'},
    ]

    assert _rpn_stack(equation) == [100.0, 200]

    assert _rpn_calc(equation) == 100


def test_exchange():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.8.3"""

    equation = [
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '1', '@Value': '660'},
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'PUSH',
         '@Value': '500'},
        {'@TermID': '1', '@SpdType': 'U', '@Multiplier': '2', '@Operation': 'EXCH'},
    ]

    assert _rpn_stack(equation) == [1320.0, 500]

    assert _rpn_calc(equation) == 1320


def test_roll_stack_down():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.8.4"""

    equation = [
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '1', '@Value': '660'},
        {'@TermID': '1', '@SpdID': 'BW01.NBAY2', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'PUSH',
         '@Value': '550'},
        {'@TermID': '1', '@SpdID': 'BW01.NBAY3', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'PUSH',
         '@Value': '500'},
        {'@TermID': '1', '@SpdType': 'U', '@Multiplier': '2', '@Operation': 'RSD'},
    ]

    assert _rpn_stack(equation) == [1320.0, 500, 550]

    assert _rpn_calc(equation) == 1320


def test_roll_stack_up():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.8.4"""

    equation = [
        {'@TermID': '1', '@SpdID': 'BW01.NBAY1', '@SpdType': 'T', '@Multiplier': '1', '@Value': '660'},
        {'@TermID': '1', '@SpdID': 'BW01.NBAY2', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'PUSH',
         '@Value': '550'},
        {'@TermID': '1', '@SpdID': 'BW01.NBAY3', '@SpdType': 'T', '@Multiplier': '1', '@Operation': 'PUSH',
         '@Value': '500'},
        {'@TermID': '1', '@SpdType': 'U', '@Multiplier': '2', '@Operation': 'RSU'},
    ]

    assert _rpn_stack(equation) == [1100.0, 660, 500]

    assert _rpn_calc(equation) == 1100


def test_pop():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.8.4"""

    equation = [
        {'@TermID': '1', '@SpdID': 'YWPS1.VYP21', '@SpdType': 'T', '@Multiplier': '1', '@Value': '100'},
        {'@TermID': '1', '@SpdID': 'YWPS2.VYP22', '@SpdType': 'T', '@Multiplier': '-1', '@Operation': 'PUSH',
         '@Value': '350'},
        {'@TermID': '1', '@SpdType': 'U', '@Multiplier': '1', '@Operation': 'POP'},
    ]

    assert _rpn_calc(equation) == 100


def test_exchange_if_less_than_or_equal_to_zero():
    """Test example from AEMO's Constraint Implementation Guidelines June 2015 Appendix A.8.4"""

    equation = [
        {'@TermID': '1', '@SpdID': 'YWPS1.VYP21', '@SpdType': 'T', '@Multiplier': '1', '@Value': '100'},
        {'@TermID': '1', '@SpdID': 'YWPS2.VYP22', '@SpdType': 'T', '@Multiplier': '-1', '@Operation': 'PUSH',
         '@Value': '350'},
        {'@TermID': '1', '@SpdType': 'S', '@Multiplier': '1', '@Operation': 'POP', '@Value': '0'},
        {'@TermID': '1', '@SpdType': 'U', '@Multiplier': '2', '@Operation': 'EXLEZ'},
    ]

    assert _rpn_calc(equation) == 200
