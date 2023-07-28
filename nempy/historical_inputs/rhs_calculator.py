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
        x = 1

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
                    entry = scada_type_set['BadScadaValuesCollection']['ScadaValues']
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
    for term in equation:
        if '@GroupID' in term and term['@GroupID'] not in ignore_groups:
            group = collect_group(equation, term['@GroupID'])
            group_result = rpn_calc(group)
            ignore_groups.append(term['@GroupID'])
        elif '@GroupID' not in term:
            if group_result is not None:
                stack.insert(0, group_result * float(term['@Multiplier']))
                group_result = None
            elif term['@SpdType'] == 'U' and '@Value' not in term:
                if len(stack) == 0:
                    stack.append(0.0)
                stack[0] = stack[0] * float(term['@Multiplier'])
            elif '@Operation' not in term:
                if len(stack) == 0:
                    stack.append(0.0)
                if '@Value' in term:
                    stack[0] += float(term['@Multiplier']) * float(term['@Value'])
                else:
                    stack[0] += float(term['@Multiplier'])
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





