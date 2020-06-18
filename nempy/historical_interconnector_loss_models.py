import pandas as pd
import numpy as np


class HistoricalInterconnectorLossModels:
    def __init__(self, inputs_manager, interval):
        self.inputs_manager = inputs_manager
        self.interval = interval



        bass_link, loss_functions = self._split_out_bass_link(loss_functions)
        bass_link = hi.split_interconnector_loss_functions_into_two_directional_links(bass_link)
        loss_functions = pd.concat([loss_functions, bass_link])

        bass_link, interpolation_break_points = self._split_out_bass_link(interpolation_break_points)
        bass_link = hi.split_interconnector_interpolation_break_points_into_two_directional_links(bass_link)
        interpolation_break_points = pd.concat([interpolation_break_points, bass_link])

    @staticmethod
    def _split_out_bass_link(interconnectors):
        bass_link = interconnectors[interconnectors['interconnector'] == 'T-V-MNSP1']
        interconnectors = interconnectors[interconnectors['interconnector'] != 'T-V-MNSP1']
        return bass_link, interconnectors
