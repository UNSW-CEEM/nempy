from nempy.historical_inputs import xml_cache


class RHSCalc:
    def __init__(self, xml):
        """

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager.xml)

        Parameters
        ----------
        xml

        Returns
        -------

        """
        inputs = xml['NEMSPDCaseFile']['NemSpdInputs']
        self.scada_data = self._reformat_scada_data(inputs['ConstraintScadaDataCollection']['ConstraintScadaData'])
        self.generic_equations = self._format_generic_equations(inputs['GenericEquationCollection']['GenericEquation'])
        self.rhs_constraint_equations = self._format_rhs_constraint_equations(
            inputs['GenericConstraintCollection']['GenericConstraint'])
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
                term['@Value'] = self._resolve_term_value(term)
            else:
                raise ValueError('Term value already present')
        return equation

    def _resolve_term_value(self, term):
        spd_id = term['@SpdID']
        if spd_id in self.generic_equations:
            value = self._compute_generic_equation(spd_id)
        elif spd_id in self.scada_data:
            value = self.scada_data[spd_id]['@Value']
            if not self.scada_data[spd_id]['@GoodValues']:
                raise ValueError('Scada data not good value')
        elif '@Default' in term:
            value = term['@Default']
        else:
            raise ValueError('Equation value could not be resolved.')
        return value

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


def rpn_calc(equation):
    stack = []
    ignore_groups = []
    group_result = None
    multi_term_operators = ['ADD', 'SUB', 'MUL', 'DIV', 'MAX', 'MIN']
    for i, term in enumerate(equation):
        if '@GroupID' in term and term['@GroupID'] not in ignore_groups:
            # Groups are evaluate separately with their own stack. If a group exists we extract it from the main
            # equation calculate the result for the group and then save the group id in a list of groups to ignore
            # so that terms in this group are skipped in future iterations of the for loop.
            group = collect_group(equation, term['@GroupID'])
            group_result = rpn_calc(group)
            ignore_groups.append(term['@GroupID'])
        elif '@GroupID' not in term:
            if group_result is not None:
                # If a group result has just been calculated the next term is treated as a multiplier to be applied to
                # the group result. Then the final value is added to the stack. See AEMO Constraint Implementation
                # Guidelines section A.3 Groups.
                stack.insert(0, group_result * float(term['@Multiplier']))
                group_result = None
            elif term['@SpdType'] == 'U' and '@Value' not in term and '@Operation' not in term:
                # If the term type is U then the multiplier is applied to the top element of the stack.  See AEMO
                # Constraint Implementation Guidelines section A.2 Top stack element.
                if len(stack) == 0:
                    stack.insert(0, 0.0)
                stack[0] = stack[0] * float(term['@Multiplier'])
            elif '@Operation' not in term:
                if (i == len(equation) - 1 or '@Operation' not in equation[i + 1] or
                        equation[i + 1]['@Operation'] not in multi_term_operators):
                    # If there is no operator in the term, and the next term is not a multi term operator then value of
                    # the term has the multiplier applied and is added to the top of the stack. See AEMO Constraint
                    # Implementation Guidelines section A.2 No RPN operators.
                    if len(stack) == 0:
                        stack.insert(0, 0.0)
                    if '@Value' in term:
                        stack[0] += float(term['@Multiplier']) * float(term['@Value'])
                    else:
                        stack[0] += float(term['@Multiplier'])
                elif (i < len(equation) - 1 and '@Operation' in equation[i + 1]
                      and equation[i + 1]['@Operation'] not in multi_term_operators):
                    # If the next term is a multi term operator then apply that operation to the current term and the
                    # next term.
                    if equation[i + 1]['@Operation'] == 'ADD':
                        stack.insert(0, (float(term['@Value']) + float(equation[i + 1]['@Value'])) *
                                     float(equation[i + 1]['@Multiplier']))
                        raise ValueError('Need to add something to skip next term.')
                else:
                    raise ValueError('Undefined RPN behaviour')
            elif term['@Operation'] == 'ADD':
                if len(stack) < 2:
                    raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
                next_top_element = (stack[0] + stack[1]) * float(term['@Multiplier'])
                stack.pop(0)
                stack[0] = next_top_element

            elif term['@Operation'] == 'STEP':
                # For terms that are STEP operators if their value is greater than zero result is the value with the
                # multiplier applied otherwise zero is returned. See AEMO Constraint Implementation Guidelines section
                # A.6.1 Step function.
                if term['@SpdType'] == 'U':
                    if float(stack[0]) > 0.0:
                        stack[0] = float(term['@Multiplier'])
                    else:
                        stack[0] = 0.0
                else:
                    if float(term['@Value']) > 0.0:
                        stack.insert(0, float(term['@Multiplier']))
                    else:
                        stack.insert(0, 0.0)

            elif term['@Operation'] == 'PUSH':
                stack.insert(0, float(term['@Multiplier']) * float(term['@Value']))

    return stack[0]


def collect_group(equation, group_id):
    group = []
    for term in equation:
        if '@GroupID' in term and term['@GroupID'] == group_id:
            term = term.copy()
            del term['@GroupID']
            group.append(term)
    return group


def are_groups(equation):
    for term in equation:
        if '@GroupID' in term.keys():
            return True
    return False





