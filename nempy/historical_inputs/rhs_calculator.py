from nempy.historical_inputs import xml_cache


class RHSCalc:
    def __init__(self, xml_cache_manager):
        """

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager)

        Parameters
        ----------
        xml

        Returns
        -------

        """
        inputs = xml_cache_manager.xml['NEMSPDCaseFile']['NemSpdInputs']
        self.scada_data = self._reformat_scada_data(inputs['ConstraintScadaDataCollection']['ConstraintScadaData'])
        self.generic_equations = self._format_generic_equations(inputs['GenericEquationCollection']['GenericEquation'])
        self.rhs_constraint_equations = self._format_rhs_constraint_equations(
            inputs['GenericConstraintCollection']['GenericConstraint'])
        self.unit_initial_mw = self._format_initial_conditions(xml_cache_manager.get_unit_initial_conditions())
        self.nemde_rhs_values = self._format_nemde_rhs_values(xml_cache_manager.get_constraint_rhs())
        self._resolved_values = {}

    @staticmethod
    def _reformat_scada_data(scada_data):
        new_format = {}

        def add_entry(new_format_dict, entry, good_data):
            new_format_dict[entry['@SpdID']] = {k: v for (k, v) in entry.items() if k != '@SpdID'}
            new_format_dict[entry['@SpdID']]['@SpdType'] = scada_type_set['@SpdType']
            new_format_dict[entry['@SpdID']]['@GoodValues'] = good_data

        for scada_type_set in scada_data:
            if type(scada_type_set['ScadaValuesCollection']['ScadaValues']) == list:
                for entry in scada_type_set['ScadaValuesCollection']['ScadaValues']:
                    add_entry(new_format, entry, True)
            else:
                entry = scada_type_set['ScadaValuesCollection']['ScadaValues']
                add_entry(new_format, entry, True)

            if 'BadScadaValuesCollection' in scada_type_set:
                if type(scada_type_set['BadScadaValuesCollection']['ScadaValues']) == list:
                    for entry in scada_type_set['BadScadaValuesCollection']['ScadaValues']:
                        add_entry(new_format, entry, False)
                else:
                    entry = scada_type_set[('BadScadaValuesCollection'
                                            '')]['ScadaValues']
                    add_entry(new_format, entry, False)

        return new_format

    @staticmethod
    def _format_generic_equations(generic_equations):
        new_format = {}
        for entry in generic_equations:
            if 'RHSTermCollection' in entry:
                terms = entry['RHSTermCollection']['RHSTerm']
                if type(terms) != list:
                    terms = [terms]
                new_format[entry['@EquationID']] = terms
        return new_format

    @staticmethod
    def _format_rhs_constraint_equations(constraints):
        new_format = {}
        for entry in constraints:
            if 'RHSTermCollection' in entry:
                terms = entry['RHSTermCollection']['RHSTerm']
                if type(terms) != list:
                    terms = [terms]
                new_format[entry['@ConstraintID']] = terms
        return new_format

    @staticmethod
    def _format_initial_conditions(initial_conditions):
        return initial_conditions.set_index('DUID')['INITIALMW'].to_dict()

    @staticmethod
    def _format_nemde_rhs_values(constraints):
        return constraints.set_index('set')['rhs'].to_dict()

    def get_rhs_equations_that_dont_reference_generic_equations(self):
        equations_to_return = []
        for equation in self.rhs_constraint_equations.keys():
            references_generic = False
            for term in self.rhs_constraint_equations[equation]:
                if term['@SpdID'] in self.generic_equations.keys():
                    references_generic = True
            if not references_generic:
                equations_to_return.append(equation)
        return equations_to_return

    def get_nemde_rhs(self, constraint_id):
        return float(self.nemde_rhs_values[constraint_id])

    def compute_constraint_rhs(self, constraint_id):
        """

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager.xml)
        >>> rhs_calculator.compute_constraint_rhs('X_BASSLINK_OFF')

        Parameters
        ----------
        xml

        Returns
        -------

        """
        equation = self.rhs_constraint_equations[constraint_id]
        equation = self._resolve_term_values(equation)
        rhs = rpn_calc(equation)
        return rhs

    def _resolve_term_values(self, equation):
        for term in equation:
            if '@Value' not in term:
                value, default_flag = self._resolve_term_value(term)
                if value is not  None:
                    term["@Value"] = value
            else:
                raise ValueError('Term value already present')
        return equation

    def _resolve_term_value(self, term):
        spd_id = term['@SpdID']
        default_flag = False
        if spd_id in self.generic_equations:
            value = self._compute_generic_equation(spd_id)
        elif spd_id in self.scada_data:
            value = self.scada_data[spd_id]['@Value']
            if not self.scada_data[spd_id]['@GoodValues']:
                raise ValueError('Scada data not good value')
        elif spd_id in self.unit_initial_mw:
            value = self.unit_initial_mw[spd_id]
        elif '@Default' in term:
            # if term['@SpdType'] == 'U':
            #     value = term['@Default']
            # else:
            #     value = 1
            value = None
            default_flag = True

        else:
            raise ValueError('Equation value could not be resolved.')
        return value, default_flag

    def _compute_generic_equation(self, equation_id):
        """

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager.xml)
        >>> rhs_calculator._compute_generic_equation('X_BASSLINK_OFF')

        Parameters
        ----------
        xml

        Returns
        -------

        """
        equation = self.generic_equations[equation_id]
        equation = self._resolve_term_values(equation)
        rhs = rpn_calc(equation)
        return rhs


def rpn_stack(equation):
    stack = []
    # ignore_groups = []
    # group_result = None
    multi_term_operators = ['ADD', 'SUB', 'MUL', 'DIV', 'MAX', 'MIN']
    skip_next_term = False
    pop_flag = False
    for i, term in enumerate(equation):
        if skip_next_term:
            # If the last term was combined with a multi term operation then we skip the multi term operation because it
            # has alread been applied.
            skip_next_term = False
            assert term['@Operation'] in multi_term_operators
            continue
        elif term['@SpdType'] == 'G':
            # Groups are evaluate separately with their own stack. If a group exists we extract it from the main
            # equation calculate the result for the group and then save the group id in a list of groups to ignore
            # so that terms in this group are skipped in future iterations of the for loop.
            group_id = equation[i+1]['@GroupTerm']
            group = collect_group(equation, group_id)
            group_result = rpn_calc(group)
            # ignore_groups.append(term['@GroupID'])
            if len(stack) == 0 or ('@Operation' in term and term['@Operation'] == 'PUSH'):
                stack.insert(0, group_result * float(term['@Multiplier']))
            else:
                stack[0] += group_result * float(term['@Multiplier'])
        # elif '@GroupID' in term and term['@GroupID'] not in ignore_groups:
        #
        #     group = collect_group(equation, term['@GroupID'])
        #     group_result = rpn_calc(group)
        elif '@GroupTerm' not in term:
            # if group_result is not None:
            #     # If a group result has just been calculated the next term is treated as a multiplier to be applied to
            #     # the group result. Then the final value is added to the stack. See AEMO Constraint Implementation
            #     # Guidelines section A.3 Groups.
            #     stack.insert(0, group_result * float(term['@Multiplier']))
            #     group_result = None
            if term['@SpdType'] == 'U' and '@Value' not in term and '@Operation' not in term:
                stack = type_u_no_operator(stack, term)
            elif term['@SpdType'] == 'B':
                stack = branching(stack, term, equation)
            elif '@Operation' not in term:
                if (1 == len(equation[i:]) or '@Operation' not in equation[i + 1] or equation[i + 1]['@Operation']
                        not in multi_term_operators or (equation[i + 1]['@Operation'] in multi_term_operators and
                                                        equation[i + 1]['@SpdType'] == 'U')):
                    stack = no_operator(stack, term)
                elif (1 < len(equation[i:]) and '@Operation' in equation[i + 1]
                      and equation[i + 1]['@Operation'] in multi_term_operators and equation[i + 1]['@SpdType'] != 'U'):
                    # If the next term is a multi term operator then apply that operation to the current term and the
                    # next term.
                    if equation[i + 1]['@Operation'] == 'ADD':
                        add_on_equation(stack, term, equation[i + 1])
                    if equation[i + 1]['@Operation'] == 'SUB':
                        subtract_on_equation(stack, term, equation[i + 1])
                    if equation[i + 1]['@Operation'] == 'MUL':
                        multipy_on_equation(stack, term, equation[i + 1])
                    if equation[i + 1]['@Operation'] == 'DIV':
                        divide_on_equation(stack, term, equation[i + 1])
                    if equation[i + 1]['@Operation'] == 'MAX':
                        max_on_equation(stack, term, equation[i + 1])
                    if equation[i + 1]['@Operation'] == 'MIN':
                        min_on_equation(stack, term, equation[i + 1])
                    skip_next_term = True
                else:
                    raise ValueError('Undefined RPN behaviour')
            elif term['@Operation'] == 'ADD' and term['@SpdType'] == 'U':
                stack = add_on_stack(stack, term)
            elif term['@Operation'] == 'SUB' and term['@SpdType'] == 'U':
                stack = subtract_on_stack(stack, term)
            elif term['@Operation'] == 'MUL' and term['@SpdType'] == 'U':
                stack = multipy_on_stack(stack, term)
            elif term['@Operation'] == 'DIV' and term['@SpdType'] == 'U':
                stack = divide_on_stack(stack, term)
            elif term['@Operation'] == 'MAX' and term['@SpdType'] == 'U':
                stack = max_on_stack(stack, term)
            elif term['@Operation'] == 'MAX':
                stack = hybrid_max(stack, term)
            elif term['@Operation'] == 'MIN' and term['@SpdType'] == 'U':
                stack = min_on_stack(stack, term)
            elif term['@Operation'] == 'MIN':
                stack = hybrid_min(stack, term)
            elif term['@Operation'] == 'STEP':
                stack = step(stack, term)
            elif term['@Operation'] == 'POW2':
                stack = square(stack, term)
            elif term['@Operation'] == 'POW3':
                stack = cube(stack, term)
            elif term['@Operation'] == 'SQRT':
                stack = sqrt(stack, term)
            elif term['@Operation'] == 'ABS':
                stack = absolute_value(stack, term)
            elif term['@Operation'] == 'NEG':
                stack = negation(stack, term)
            elif term['@Operation'] == 'PUSH':
                stack = push(stack, term)
            elif term['@Operation'] == 'DUP' and term['@SpdType'] == 'U':
                stack = duplicate(stack, term)
            elif term['@Operation'] == 'EXCH' and term['@SpdType'] == 'U':
                stack = exchange(stack, term)
            elif term['@Operation'] == 'RSD' and term['@SpdType'] == 'U':
                stack = roll_stack_down(stack, term)
            elif term['@Operation'] == 'RSU' and term['@SpdType'] == 'U':
                stack = roll_stack_up(stack, term)
            elif term['@Operation'] == 'POP':
                pop_flag, stack = pop(stack, term)
            elif term['@Operation'] == 'EXLEZ' and term['@SpdType'] == 'U' and pop_flag:
                stack = exchange_if_less_than_zero(stack, term)
                pop_flag = False
    return stack


def rpn_calc(equation):
    return rpn_stack(equation)[0]


def get_default_if_needed(term):
    if '@Value' not in term:
        return term['@Default']
    else:
        return term['@Value']


def no_operator(stack, term):
    # If there is no operator in the term, and the next term is not a multi term operator then the
    # value of the term has the multiplier applied and is added to the top of the stack. See AEMO
    # Constraint Implementation Guidelines section A.2 No RPN operators.
    if len(stack) == 0:
        stack.insert(0, 0.0)
    if '@Value' in term:
        stack[0] += float(term['@Multiplier']) * float(term['@Value'])
    else:
        stack[0] += float(term['@Multiplier'])
    return stack


def type_u_no_operator(stack, term):
    # If a term is type U and has no operator then the multiplier is applied to the top element of the stack. See AEMO
    # Constraint Implementation Guidelines section A.5 Top stack element.
    if len(stack) == 0:
        stack.insert(0, 0.0)
    stack[0] = stack[0] * float(term['@Multiplier'])
    return stack


def step(stack, term):
    # For terms that are STEP operators if their value is greater than zero result is the value with the
    # multiplier applied otherwise zero is returned. See AEMO Constraint Implementation Guidelines section
    # A.6.1 Step function.
    if term['@SpdType'] == 'U':
        # If type U then apply the STEP operation to the element on top of the stack.
        if float(stack[0]) > 0.0:
            stack[0] = float(term['@Multiplier'])
        else:
            stack[0] = 0.0
    else:
        # If not type U the apply the STEP operation to the term value.
        if float(term['@Value']) > 0.0:
            stack.insert(0, float(term['@Multiplier']))
        else:
            stack.insert(0, 0.0)
    return stack


def square(stack, term):
    # For terms that are POW2 operators either the term value or the top stack value is squared. See AEMO Constraint
    # Implementation Guidelines section A.6.2 Square.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 2 * float(term['@Multiplier'])
    else:
        # If not type U the apply the STEP operation to the term value.
        stack.insert(0, float(term['@Value']) ** 2 * float(term['@Multiplier']))
    return stack


def cube(stack, term):
    # For terms that are POW3 operators either the term value or the top stack value is cubed. See AEMO Constraint
    # Implementation Guidelines section A.6.3 Cube.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 3 * float(term['@Multiplier'])
    else:
        # If not type U the apply the STEP operation to the term value.
        stack.insert(0, float(term['@Value']) ** 3 * float(term['@Multiplier']))
    return stack


def sqrt(stack, term):
    # For terms that are SQRT operators either the term value or the top stack value is square rooted. See AEMO
    # Constraint Implementation Guidelines section A.6.4 Square Root.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 0.5 * float(term['@Multiplier'])
    else:
        # If not type U the apply the STEP operation to the term value.
        stack.insert(0, float(term['@Value']) ** 0.5 * float(term['@Multiplier']))
    return stack


def absolute_value(stack, term):
    # For terms that are ABS operators either the absolute value of the term value or the top stack value. See AEMO
    # Constraint Implementation Guidelines section A.6.5 Absolute Value.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = abs(stack[0]) * float(term['@Multiplier'])
    else:
        # If not type U the apply the STEP operation to the term value.
        stack.insert(0, abs(float(term['@Value'])) * float(term['@Multiplier']))
    return stack


def negation(stack, term):
    # For terms that are NEG operators either the term value or the top stack value is negated. See AEMO Constraint
    # Implementation Guidelines section A.6.5 Negation.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = -1.0 * stack[0] * float(term['@Multiplier'])
    else:
        # If not type U the apply the STEP operation to the term value.
        stack.insert(0, -1.0 * float(term['@Value']) * float(term['@Multiplier']))
    return stack


def add_on_stack(stack, term):
    # Where an ADD operation is encountered in the equation with no previous term, without an operator, for the
    # ADD operation to act on, then the ADD operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[0] + stack[1]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def add_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an ADD operator, then the term value
    # and the ADD operator value are summed and added to the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, (float(value_one) + float(value_two)) * float(next_term['@Multiplier']))
    return stack


def subtract_on_stack(stack, term):
    # Where an SUB operation is encountered in the equation with no previous term, without an operator, for the
    # SUB operation to act on, then the SUB operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] - stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def subtract_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an SUB operator, then
    # SUB operator value is subtracted from the term value and the result added to the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.2 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, (float(value_one) - float(value_two)) * float(next_term['@Multiplier']))
    return stack


def multipy_on_stack(stack, term):
    # Where an MUL (multiply) operation is encountered in the equation with no previous term, without an operator, for
    # the MUL operation to act on, then the MUL operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.3 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] * stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def multipy_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an MUL operator, then
    # MUL operator value is multiplied with the term value and the result added to the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.3 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, (float(value_one) * float(value_two)) * float(next_term['@Multiplier']))
    return stack


def divide_on_stack(stack, term):
    # Where an DIV (divide) operation is encountered in the equation with no previous term, without an operator, for
    # the DIV operation to act on, then the DIV operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.4 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] / stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def divide_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an DIV operator, then
    # the term value is divide by DIV operator value and the result added to the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.4 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, (float(value_one) / float(value_two)) * float(next_term['@Multiplier']))
    return stack


def max_on_stack(stack, term):
    # Where an MAX (maximum) operation is encountered in the equation with no previous term, without an operator, for
    # the MAX operation to act on, then the MAX operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.5 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = max(stack[1], stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def max_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an MAX operator, then
    # the maximum of the term value and the operator value is taken and added to the top of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.5 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, max(float(value_one), float(value_two)) * float(next_term['@Multiplier']))
    return stack


def hybrid_max(stack, term):
    value_one = get_default_if_needed(term)
    next_top_element = max(float(value_one), stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def min_on_stack(stack, term):
    # Where an MIN (minimum) operation is encountered in the equation with no previous term, without an operator, for
    # the MIN operation to act on, then the MIN operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.6 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = min(stack[1], stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def hybrid_min(stack, term):
    value_one = get_default_if_needed(term)
    next_top_element = min(float(value_one), stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def min_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an MIN operator, then
    # the minimum of the term value and the operator value is taken and added to the top of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.6 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, min(float(value_one), float(value_two)) * float(next_term['@Multiplier']))
    return stack


def push(stack, term):
    # If a push operator is given the value of the operator with the multiplier applied is added to the top of the
    # stack.
    # See AEMO Constraint Implementation Guidelines section A.8.1 Push.
    if term['@SpdType'] not in ['C', 'G']:  # Condition found through empirical testing
        stack.insert(0, float(term['@Multiplier']) * float(term['@Value']))
    else:
        stack.insert(0, float(term['@Multiplier']))
    return stack


def duplicate(stack, term):
    # If a DUP operator is given the value at the top of the stack is duplicated, the multiplier is applied and
    # the term is added to the top of the stack.
    # See AEMO Constraint Implementation Guidelines section A.8.2 Duplicate.
    stack.insert(0, stack[0] * float(term['@Multiplier']))
    return stack


def exchange(stack, term):
    # If a EXCH operator is given the top and second top elements of the stacked are swapped.
    # See AEMO Constraint Implementation Guidelines section A.8.3 Exchange.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    top_element = stack.pop(0)
    stack.insert(1, top_element)
    stack.insert(0, stack.pop(0) * float(term['@Multiplier']))
    return stack


def roll_stack_down(stack, term):
    # If a RSD operator is given the bottom element of the stack is moved to the top and the multiplier is applied.
    # See AEMO Constraint Implementation Guidelines section A.8.3 Exchange.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    bottom_element = stack.pop(-1)
    stack.insert(0, bottom_element * float(term['@Multiplier']))
    return stack


def roll_stack_up(stack, term):
    # If a RSU operator is given the top element of the stack is moved to the bottom and the multiplier is applied to
    # the new top element.
    # See AEMO Constraint Implementation Guidelines section A.8.3 Exchange.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    top_element = stack.pop(0)
    stack.append(top_element)
    top_element = stack.pop(0)
    stack.insert(0, top_element * float(term['@Multiplier']))
    return stack


def pop(stack, term):
    # See AEMO Constraint Implementation Guidelines section A.9.1 and A.9.2.
    if term['@SpdType'] == 'U':
        # If the POP operator is given and the term is of type U the top element of the stack is removed. If the element
        # that was popped was less than of equal to zero than the POP flag is set to true.
        top_element = stack.pop(0)
        if top_element <= 0.0:
            pop_flag = True
        else:
            pop_flag = False
    else:
        # If the POP operator is given and the term is not of type U the top element of the stack is not removed. If
        # the term value is less than zero then the POP flag is set to true.
        if float(term['@Value']) <= 0.0:
            pop_flag = True
        else:
            pop_flag = False
    return pop_flag, stack


def exchange_if_less_than_zero(stack, term):
    # If the EXLEZ is given and the pop flag is true then the top two elements are exchanged.
    # See AEMO Constraint Implementation Guidelines section A.9.2
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    top_element = stack.pop(0)
    stack.insert(1, top_element)
    stack.insert(0, stack.pop(0) * float(term['@Multiplier']))
    return stack


def branching(stack, term, equation):
    # If a term with type B is given, we use the terms @ParameterTerm1 to retrieve the value of the term in the equation
    # with the matching term ID. If the value of that term is greater than 0.0, then we return the value of the term
    # Specified by @ParameterTerm2 else we return the value specified by @ParameterTerm3.
    # See AEMO Constraint Implementation Guidelines section A.9.3
    if float(equation[int(term['@ParameterTerm1']) - 1]['@Value']) > 0.0:
        stack.insert(0, float(equation[int(term['@ParameterTerm2']) - 1]['@Value']) * float(term['@Multiplier']))
    else:
        stack.insert(0, float(equation[int(term['@ParameterTerm3']) - 1]['@Value']) * float(term['@Multiplier']))
    return stack


def collect_group(equation, group_id):
    group = []
    for term in equation:
        if '@GroupTerm' in term and term['@GroupTerm'] == group_id:
            term = term.copy()
            del term['@GroupTerm']
            group.append(term)
    return group


def are_groups(equation):
    for term in equation:
        if '@GroupTerm' in term.keys():
            return True
    return False





