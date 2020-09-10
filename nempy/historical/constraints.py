import pandas as pd


class ConstraintData:
    def __init__(self, raw_inputs_loader):
        self.raw_inputs_loader = raw_inputs_loader

        self.generic_rhs = self.raw_inputs_loader.get_constraint_rhs()
        self.generic_type = self.raw_inputs_loader.get_constraint_type()
        self.generic_rhs = pd.merge(self.generic_rhs, self.generic_type.loc[:, ['set', 'type']], on='set')
        type_map = {'LE': '<=', 'EQ': '=', 'GE': '>='}
        self.generic_rhs['type'] = self.generic_rhs['type'].apply(lambda x: type_map[x])

        bid_type_map = dict(ENOF='energy', LDOF='energy', L5RE='lower_reg', R5RE='raise_reg', R5MI='raise_5min',
                            L5MI='lower_5min', R60S='raise_60s', L60S='lower_60s', R6SE='raise_6s',
                            L6SE='lower_6s')

        self.unit_generic_lhs = self.raw_inputs_loader.get_constraint_unit_lhs()
        self.unit_generic_lhs['service'] = self.unit_generic_lhs['service'].apply(lambda x: bid_type_map[x])
        self.region_generic_lhs = self.raw_inputs_loader.get_constraint_region_lhs()
        self.region_generic_lhs['service'] = self.region_generic_lhs['service'].apply(lambda x: bid_type_map[x])

        self.interconnector_generic_lhs = self.raw_inputs_loader.get_constraint_interconnector_lhs()

        self.fcas_requirements = pd.merge(self.region_generic_lhs, self.generic_rhs, on='set')
        self.fcas_requirements = self.fcas_requirements.loc[:, ['set', 'service', 'region', 'type', 'rhs']]
        self.fcas_requirements.columns = ['set', 'service', 'region', 'type', 'volume']

    def get_rhs_and_type(self):
        return self.generic_rhs[~self.generic_rhs['set'].isin(list(self.fcas_requirements['set']))]

    def get_unit_lhs(self):
        return self.unit_generic_lhs

    def get_interconnector_lhs(self):
        return self.interconnector_generic_lhs

    def get_fcas_requirements(self):
        return self.fcas_requirements

    def get_violation_costs(self):
        return self.generic_type.loc[:, ['set', 'cost']]

    def get_constraint_violation_prices(self):
        return self.raw_inputs_loader.get_constraint_violation_prices()

    def is_over_constrained_dispatch_rerun(self):
        return self.raw_inputs_loader.is_over_constrained_dispatch_rerun()

