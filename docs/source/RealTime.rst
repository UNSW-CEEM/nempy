The RealTime markets class
===============================
A model of the NEM real time market.

.. autoclass:: nempy.markets.RealTime

    .. automethod:: set_unit_energy_volume_bids(self, volume_bids)
    .. automethod:: set_unit_energy_price_bids(self, price_bids)

Functions
=========
.. autofunction:: nempy.variable_ids.energy(capacity_bids, next_variable_id)
.. autofunction:: nempy.objective_function.energy(variable_ids, price_bids)
.. autofunction:: nempy.objective_function.scale_by_loss_factors(objective_function, unit_info)
